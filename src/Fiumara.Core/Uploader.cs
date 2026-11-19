using System.Diagnostics;
using System.Net;
using Fiumara.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Fiumara.Core;

internal sealed partial class Uploader(PieceManager pieces, PieceStorage storage, ILogger<Uploader> logger)
{
    public const int MaxBlockLength = 16 * 1024;
    public const int MaxQueuedRequests = 250;
    public const int MaxBlocksInFlight = 4;

    public async Task OnRequestAsync(PeerConnection peer, PeerState state, BlockRequest request)
    {
        if (Validate(request) is { } problem)
        {
            Log.InvalidRequest(logger, peer.Endpoint, request.PieceIndex, request.Offset, request.Length, problem);
            peer.Disconnect();
            return;
        }

        if (state.AmChoking)
        {
            Log.RequestWhileChoked(logger, peer.Endpoint, request.PieceIndex, request.Offset);
            return;
        }

        if (state.IncomingRequests.Count >= MaxQueuedRequests || state.IncomingRequests.Contains(request))
        {
            Log.RequestIgnored(logger, peer.Endpoint, request.PieceIndex, request.Offset);
            return;
        }

        state.IncomingRequests.Add(request);
        await ServeAsync(peer, state);
    }

    public void OnCancel(PeerState state, BlockRequest request) => state.IncomingRequests.Remove(request);

    public async Task OnBlockSentAsync(PeerConnection peer, PeerState state, int length)
    {
        Debug.Assert(state.BlocksInFlight > 0, $"Peer {peer.Endpoint} sent a block that was never handed to it.");
        state.BlocksInFlight--;
        state.BytesSent += length;
        await ServeAsync(peer, state);
    }

    private async Task ServeAsync(PeerConnection peer, PeerState state)
    {
        while (state.BlocksInFlight < MaxBlocksInFlight && state.IncomingRequests.Count > 0)
        {
            var request = state.IncomingRequests[0];
            state.IncomingRequests.RemoveAt(0);
            var data = await storage.ReadBlockAsync(request.PieceIndex, request.Offset, request.Length);
            peer.SendPiece(request.PieceIndex, request.Offset, data);
            state.BlocksInFlight++;
            Log.BlockServed(logger, peer.Endpoint, request.PieceIndex, request.Offset, request.Length);
        }
    }

    private string? Validate(BlockRequest request)
    {
        if (request.PieceIndex < 0 || request.PieceIndex >= pieces.PieceCount)
            return "there is no such piece";
        if (!pieces.Has(request.PieceIndex))
            return "we do not have that piece";
        if (request.Length <= 0 || request.Length > MaxBlockLength)
            return $"a block is 1 to {MaxBlockLength} bytes";
        if (request.Offset < 0 || (long)request.Offset + request.Length > pieces.GetPiece(request.PieceIndex).Size)
            return "the block falls outside the piece";
        return null;
    }

    private static partial class Log
    {
        [LoggerMessage(4001, LogLevel.Warning,
            "Peer {Endpoint} requested the block at offset {Offset} of piece {PieceIndex} ({Length} bytes), but {Problem}. Disconnecting.")]
        public static partial void InvalidRequest(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset, int length, string problem);

        [LoggerMessage(4002, LogLevel.Debug,
            "Ignoring the request of peer {Endpoint} for offset {Offset} of piece {PieceIndex}: we are choking it.")]
        public static partial void RequestWhileChoked(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset);

        [LoggerMessage(4003, LogLevel.Debug,
            "Ignoring the request of peer {Endpoint} for offset {Offset} of piece {PieceIndex}: it is already queued or the queue is full.")]
        public static partial void RequestIgnored(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset);

        [LoggerMessage(4004, LogLevel.Trace,
            "Sending the block at offset {Offset} of piece {PieceIndex} ({Length} bytes) to peer {Endpoint}.")]
        public static partial void BlockServed(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset, int length);
    }
}
