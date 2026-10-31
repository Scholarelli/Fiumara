using System.Diagnostics;
using System.Net;
using System.Threading.Channels;
using Fiumara.Core.Events;
using Fiumara.Core.Protocol;
using Fiumara.Core.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Fiumara.Core;

public partial class TorrentHandle
{
    private readonly Channel<EngineEvent> _eventChannel = Channel.CreateUnbounded<EngineEvent>();
    private readonly PieceManager _pieceManager;

    private readonly TrackerClient _trackerClient;
    private readonly TorrentInfo _torrent;
    private readonly PieceStorage _storage;
    private Task _ticker = Task. CompletedTask;

    private readonly PeerSwarm _swarm;
    private readonly ILogger<TorrentHandle> _logger;
    private readonly FiumaraOptions _options;
    private readonly TimeProvider _timeProvider;
    private Task? _eventLoop;

    public Task Completion => _eventLoop
                              ?? throw new InvalidOperationException("TorrentHandle has not been started.");

    public TorrentHandle(TorrentInfo torrent, ILoggerFactory? loggerFactory = null, FiumaraOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        _torrent = torrent;
        _options = options ?? FiumaraOptions.Default;
        _options.Validate();

        var loggers = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = loggers.CreateLogger<TorrentHandle>();
        _swarm = new PeerSwarm(_torrent.InfoHash, _torrent.PieceCount, _eventChannel.Writer, _options, loggers);

        _timeProvider = timeProvider ?? TimeProvider.System;
        _trackerClient = new TrackerClient(PeerSwarm.PeerId, _options.ListenPort);

        _pieceManager = new PieceManager(_torrent, new RequestsRegister(_options.RequestTimeout, _timeProvider));
        _storage = new PieceStorage(_options.DownloadPath, _torrent.PieceLength);

    }


    public async Task StartAsync()
    {
        var trackerResponse = await _trackerClient.GetPeersAsync(_torrent);
        trackerResponse.ForEach(endpoint => _swarm.AddEndpoint(endpoint));
        _swarm.ConnectUpTo(_options.MaxPeers);

        _eventLoop = Task.Run(RunEventLoopAsync);
        _ticker = RunTickerAsync();
    }


    private async Task RunTickerAsync()
    {
        try
        {
            await foreach (var tick in Ticker.StartAsync(_options.TickPeriod, _timeProvider))
                _eventChannel.Writer.TryWrite(tick);
        }
        catch (Exception ex)
        {
            Log.TickerCrashed(_logger, ex);
            throw;
        }
    }

    private async Task RunEventLoopAsync()
    {
        try
        {
            await ProcessEventsAsync();
        }
        catch (Exception ex)
        {
            Log.EventLoopCrashed(_logger, ex);
            throw;
        }
    }

    private void HandlePeerDisconnect(PeerConnection deadPeer, PeerState state)
    {
        _pieceManager.ReleasePeerRequests(deadPeer);
        _pieceManager.RemoveAvailability(state.Has);
        _swarm.OnPeerDisconnected(deadPeer);
    }

    private async Task HandleBlockReceived(BlockReceived b)
    {
        var outcome = _pieceManager.OnBlockReceived(b.PieceIndex, b.Offset, b.Data);

        Log.BlockReceived(_logger, b.Peer.Endpoint, b.PieceIndex, b.Offset, b.Data.Length, outcome);

        switch (outcome)
        {
            case BlockOutcome.PieceVerified:
                var piece = _pieceManager.GetPiece(b.PieceIndex);
                Log.PieceVerified(_logger, piece.Index);
                await _storage.WritePieceAsync(piece.Index, piece.Buffer);
                _swarm.BroadcastHave(piece.Index);
                break;
            case BlockOutcome.PieceCorrupted:
                Log.PieceCorrupted(_logger, b.PieceIndex);
                break;
            case BlockOutcome.Rejected:
                Log.InvalidBlock(_logger, b.Peer.Endpoint, b.PieceIndex, b.Offset, b.Data.Length);
                b.Peer.Disconnect();
                break;
        }
    }


    private async Task ProcessEventsAsync()
    {
        await foreach (var engineEvent in _eventChannel.Reader.ReadAllAsync())
        {
            switch (engineEvent)
            {
                case Tick:
                    HandleTick();
                    break;
                case PeerEvent peerEvent when _swarm.TryGetState(peerEvent.Peer, out var state):
                    await HandlePeerEventAsync(peerEvent, state);
                    break;
                case PeerEvent peerEvent:
                    Log.InactivePeerEvent(_logger, peerEvent.Peer.Endpoint, peerEvent.GetType().Name);
                    break;
            }
        }
    }

    private void HandleTick()
    {
        _pieceManager.RemoveExpiredRequests();
        AssertAvailability();
        foreach (var (peer, state) in _swarm.ActivePeers)
        {
            EvaluatePeer(peer, state);
            FillPeerPipeline(peer, state);
        }
    }

    [Conditional("DEBUG")]
    private void AssertAvailability()
    {
        var peers = _swarm.ActivePeers;
        for (var i = 0; i < _torrent.PieceCount; i++)
            Debug.Assert(_pieceManager.AvailabilityOf(i) == peers.Count(p => p.State.Has[i]),
                $"Availability of piece {i} does not match the bitfields of the active peers.");
    }

    private async Task HandlePeerEventAsync(PeerEvent peerEvent, PeerState state)
    {
        switch (peerEvent)
        {
            case PeerConnected c:
                if (!_swarm.TryAdmit(c.Peer, c.RemotePeerId))
                    HandlePeerDisconnect(c.Peer, state);
                break;
            case ChokeStateChanged c:
                state.PeerChoking = c.IsChoked;
                if (c.IsChoked)
                {
                    Log.Choked(_logger, c.Peer.Endpoint);
                    _pieceManager.ReleasePeerRequests(c.Peer);
                    break;
                }
                Log.Unchoked(_logger, c.Peer.Endpoint);
                EvaluatePeer(c.Peer, state);
                FillPeerPipeline(c.Peer, state);
                break;
            case BitfieldReceived b:
                _pieceManager.RemoveAvailability(state.Has);
                state.Has = b.BitField;
                _pieceManager.AddAvailability(b.BitField);
                EvaluatePeer(b.Peer, state);
                break;
            case HaveReceived h:
                if (!state.Has[h.PieceIndex])
                {
                    state.Has[h.PieceIndex] = true;
                    _pieceManager.AddAvailability(h.PieceIndex);
                }
                EvaluatePeer(h.Peer, state);
                break;
            case PeerDisconnected d:
                HandlePeerDisconnect(d.Peer, state);
                break;
            case BlockReceived b:
                await HandleBlockReceived(b);
                FillPeerPipeline(b.Peer, state);
                break;
            case InterestedStateChanged i:
                state.PeerInterested = i.IsInterested;
                if (i.IsInterested)
                    Log.PeerInterested(_logger, i.Peer.Endpoint);
                else
                    Log.PeerNotInterested(_logger, i.Peer.Endpoint);
                break;
            default:
                Log.UnhandledEvent(_logger, peerEvent.Peer.Endpoint, peerEvent.GetType().Name);
                break;
        }
    }

    private void EvaluatePeer(PeerConnection peer, PeerState state) =>
        _swarm.SetInterest(peer, _pieceManager.GetNextPieceToDownload(state.Has) != null);

    private void FillPeerPipeline(PeerConnection peer, PeerState state)
    {
        if (state.PeerChoking)
            return;

        int maxPipelineSize = _options.MaxPipelineSize;

        while (_pieceManager.CountPendingRequests(peer) < maxPipelineSize)
        {
            if (_pieceManager.ReserveNextBlock(peer, state.Has) is not { } request) break;

            peer.RequestBlock(request.PieceIndex, request.Offset, request.Length);
            Log.BlockRequested(_logger, peer.Endpoint, request.PieceIndex, request.Offset, request.Length);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(1001, LogLevel.Error, "The ticker crashed.")]
        public static partial void TickerCrashed(ILogger logger, Exception exception);

        [LoggerMessage(1002, LogLevel.Error, "The event loop crashed.")]
        public static partial void EventLoopCrashed(ILogger logger, Exception exception);

        [LoggerMessage(1003, LogLevel.Trace,
            "Block at offset {Offset} of piece {PieceIndex} ({Length} bytes) from peer {Endpoint}: {Outcome}.")]
        public static partial void BlockReceived(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset, int length, BlockOutcome outcome);

        [LoggerMessage(1004, LogLevel.Information, "Piece {PieceIndex} verified, writing it to disk.")]
        public static partial void PieceVerified(ILogger logger, int pieceIndex);

        [LoggerMessage(1005, LogLevel.Warning,
            "Piece {PieceIndex} failed the hash check and will be downloaded again.")]
        public static partial void PieceCorrupted(ILogger logger, int pieceIndex);

        [LoggerMessage(1006, LogLevel.Warning,
            "Peer {Endpoint} sent an invalid block (piece {PieceIndex}, offset {Offset}, {Length} bytes). Disconnecting.")]
        public static partial void InvalidBlock(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset, int length);

        [LoggerMessage(1007, LogLevel.Debug, "Ignoring {EventName} from inactive peer {Endpoint}.")]
        public static partial void InactivePeerEvent(ILogger logger, IPEndPoint endpoint, string eventName);

        [LoggerMessage(1008, LogLevel.Information, "Peer {Endpoint} choked us.")]
        public static partial void Choked(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(1009, LogLevel.Information, "Peer {Endpoint} unchoked us.")]
        public static partial void Unchoked(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(1010, LogLevel.Debug, "Peer {Endpoint} is interested in us.")]
        public static partial void PeerInterested(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(1011, LogLevel.Debug, "Peer {Endpoint} is no longer interested in us.")]
        public static partial void PeerNotInterested(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(1012, LogLevel.Warning, "Unhandled {EventName} from peer {Endpoint}.")]
        public static partial void UnhandledEvent(ILogger logger, IPEndPoint endpoint, string eventName);

        [LoggerMessage(1013, LogLevel.Trace,
            "Requesting the block at offset {Offset} of piece {PieceIndex} ({Length} bytes) from peer {Endpoint}.")]
        public static partial void BlockRequested(ILogger logger, IPEndPoint endpoint, int pieceIndex, int offset, int length);
    }
}
