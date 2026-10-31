using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text;
using System.Threading.Channels;
using Fiumara.Core.Events;
using Microsoft.Extensions.Logging;

namespace Fiumara.Core;

internal sealed class PeerState(int pieceCount)
{
    public bool PeerChoking { get; set; } = true; // il peer ci sta chokando
    public bool PeerInterested { get; set; } // il peer è interessato a noi
    public bool AmInterested { get; set; } // noi siamo interessati al peer
    public bool[] Has { get; set; } = new bool[pieceCount];
    public byte[]? RemotePeerId { get; set; }
}

internal sealed record ActivePeer(PeerConnection Connection, PeerState State);

internal partial class PeerSwarm
{
    public static readonly string PeerId = $"-FMR0001-{Random.Shared.Next(1000000000):D11}";
    private static readonly byte[] PeerIdBytes = Encoding.ASCII.GetBytes(PeerId);
    private readonly Queue<IPEndPoint> _waitingRoom = [];
    private readonly Dictionary<PeerConnection, PeerState> _activePeers = new(ReferenceEqualityComparer.Instance);
    private readonly ISwarmOptions _options;
    private readonly ChannelWriter<EngineEvent> _eventChannel;
    private readonly IReadOnlyList<byte> _infoHash;
    private readonly int _pieceCount;
    private readonly ILoggerFactory _loggers;
    private readonly ILogger<PeerSwarm> _logger;


    internal PeerSwarm(IReadOnlyList<byte> infoHash, int pieceCount, ChannelWriter<EngineEvent> events,
        ISwarmOptions options, ILoggerFactory loggers)
    {
        _options = options;
        _loggers = loggers;
        _logger = loggers.CreateLogger<PeerSwarm>();
        _eventChannel = events;
        _infoHash = infoHash;
        _pieceCount = pieceCount;
    }

    public IReadOnlyCollection<ActivePeer> ActivePeers =>
        _activePeers.Select(peer => new ActivePeer(peer.Key, peer.Value)).ToList();
    public int ActivePeerCount => _activePeers.Count;

    public bool TryGetState(PeerConnection peer, [NotNullWhen(true)] out PeerState? state) =>
        _activePeers.TryGetValue(peer, out state);

    private void RemovePeer(PeerConnection connection)
    {
        if (_activePeers.Remove(connection))
            Log.PeerRemoved(_logger, connection.Endpoint);
        else
            Log.PeerNotFound(_logger, connection.Endpoint);
    }

    public void AddEndpoint(IPEndPoint endpoint)
    {
        if (_activePeers.Keys.Any(peer => peer.Endpoint.Equals(endpoint)))
        {
            Log.AlreadyConnected(_logger, endpoint);
            return;
        }

        if (_waitingRoom.Contains(endpoint))
        {
            Log.AlreadyWaiting(_logger, endpoint);
            return;
        }

        _waitingRoom.Enqueue(endpoint);
    }

    public void ConnectUpTo(int targetCount)
    {
        for (int i = 0; i < targetCount; i++)
        {
            if (!_waitingRoom.TryDequeue(out var endpoint))
            {
                Log.WaitingRoomEmpty(_logger);
                break;
            }


            var peer = new PeerConnection(endpoint, [.. _infoHash], PeerId, _eventChannel, _pieceCount,
                _loggers.CreateLogger<PeerConnection>());
            _activePeers.Add(peer, new PeerState(_pieceCount));

            _ = SafeConnectAsync(peer, endpoint);
        }
    }

    private async Task SafeConnectAsync(PeerConnection peer, IPEndPoint endpoint)
    {
        try
        {
            await peer.ConnectAsync(_options.HandshakeTimeout);
        }
        catch (Exception ex)
        {
            Log.ConnectFailed(_logger, endpoint, ex.Message);

            // Let the handle clean up the dead peer and optionally spawn a replacement
            _eventChannel.TryWrite(new PeerDisconnected(peer));
        }
    }

    public bool TryAdmit(PeerConnection peer, byte[] remotePeerId)
    {
        if (!TryGetState(peer, out var state))
            return false;

        if (remotePeerId.AsSpan().SequenceEqual(PeerIdBytes))
        {
            Log.SelfConnection(_logger, peer.Endpoint);
            return false;
        }

        if (_activePeers.Values.Any(other => other.RemotePeerId is { } id && id.AsSpan().SequenceEqual(remotePeerId)))
        {
            Log.DuplicatePeerId(_logger, peer.Endpoint);
            return false;
        }

        state.RemotePeerId = remotePeerId;
        return true;
    }

    public void OnPeerDisconnected(PeerConnection peer)
    {
        peer.Dispose();
        RemovePeer(peer);
        ConnectUpTo(1);
    }

    public void BroadcastHave(int pieceIndex)
    {
        var sent = 0;
        foreach (var (peer, state) in _activePeers)
        {
            if (state.Has[pieceIndex]) continue;
            peer.SendHave(pieceIndex);
            sent++;
        }

        Log.HaveBroadcast(_logger, pieceIndex, sent);
    }

    public void SetInterest(PeerConnection peer, bool interested)
    {
        if (!TryGetState(peer, out var state) || state.AmInterested == interested)
            return;

        state.AmInterested = interested;
        peer.SendInterested(interested);
    }

    private static partial class Log
    {
        [LoggerMessage(2001, LogLevel.Debug, "Peer {Endpoint} removed from active peers.")]
        public static partial void PeerRemoved(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2002, LogLevel.Warning, "Peer {Endpoint} was not found in active peers.")]
        public static partial void PeerNotFound(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2003, LogLevel.Debug, "Peer {Endpoint} is already connected.")]
        public static partial void AlreadyConnected(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2004, LogLevel.Debug, "Peer {Endpoint} is already in the waiting room.")]
        public static partial void AlreadyWaiting(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2005, LogLevel.Information, "No more peers available in the waiting room.")]
        public static partial void WaitingRoomEmpty(ILogger logger);

        [LoggerMessage(2006, LogLevel.Information, "Could not connect to peer {Endpoint}: {Reason}")]
        public static partial void ConnectFailed(ILogger logger, IPEndPoint endpoint, string reason);

        [LoggerMessage(2007, LogLevel.Information, "Peer {Endpoint} is ourselves. Disconnecting.")]
        public static partial void SelfConnection(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2008, LogLevel.Information,
            "Peer {Endpoint} is already connected through another address. Disconnecting.")]
        public static partial void DuplicatePeerId(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2009, LogLevel.Debug, "Sent HAVE for piece {PieceIndex} to {Count} peers.")]
        public static partial void HaveBroadcast(ILogger logger, int pieceIndex, int count);
    }
}
