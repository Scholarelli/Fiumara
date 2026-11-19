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
    public bool AmChoking { get; set; } = true;
    public long BytesReceived { get; set; }
    public long BytesReceivedAtRechoke { get; set; }
    public long BytesSent { get; set; }
    public List<BlockRequest> IncomingRequests { get; } = [];
    public int BlocksInFlight { get; set; }
    public bool[] Has { get; set; } = new bool[pieceCount];
    public byte[]? RemotePeerId { get; set; }
    public long? SnubbedSince { get; set; }
    public bool IsSnubbed => SnubbedSince.HasValue;
    public bool IsAdmitted => RemotePeerId is not null;
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
    private readonly TimeProvider _timeProvider;
    private long? _lastRechoke;
    private long? _lastOptimisticRotation;
    private PeerConnection? _optimistic;


    internal PeerSwarm(IReadOnlyList<byte> infoHash, int pieceCount, ChannelWriter<EngineEvent> events,
        ISwarmOptions options, ILoggerFactory loggers, TimeProvider timeProvider)
    {
        _options = options;
        _loggers = loggers;
        _logger = loggers.CreateLogger<PeerSwarm>();
        _eventChannel = events;
        _infoHash = infoHash;
        _pieceCount = pieceCount;
        _timeProvider = timeProvider;
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

    public void Snub(PeerConnection peer)
    {
        if (!TryGetState(peer, out var state) || state.IsSnubbed)
            return;

        state.SnubbedSince = _timeProvider.GetTimestamp();
        Log.PeerSnubbed(_logger, peer.Endpoint);
    }

    public void Unsnub(PeerConnection peer)
    {
        if (!TryGetState(peer, out var state) || !state.IsSnubbed)
            return;

        state.SnubbedSince = null;
        Log.PeerUnsnubbed(_logger, peer.Endpoint);
    }

    public void DisconnectExpiredSnubs()
    {
        var now = _timeProvider.GetTimestamp();
        foreach (var (peer, state) in _activePeers)
        {
            if (state.SnubbedSince is not { } since
                || _timeProvider.GetElapsedTime(since, now) <= _options.SnubTimeout)
                continue;

            Log.SnubTimedOut(_logger, peer.Endpoint, _options.SnubTimeout);
            peer.Disconnect();
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
            if (state.Has[pieceIndex] || !state.IsAdmitted) continue;
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
        if (!interested)
            Unsnub(peer);
    }

    public void RechokeIfDue()
    {
        var now = _timeProvider.GetTimestamp();
        if (_lastRechoke is not null && _timeProvider.GetElapsedTime(_lastRechoke.Value, now) < _options.RechokeInterval)
            return;

        var rotate = _lastOptimisticRotation is null
                     || _timeProvider.GetElapsedTime(_lastOptimisticRotation.Value, now) >= _options.OptimisticUnchokeInterval;
        var candidates = _activePeers
            .Where(peer => peer.Value.IsAdmitted)
            .Select(peer => new ChokeCandidate(peer.Key, peer.Value.PeerInterested, peer.Value.IsSnubbed,
                peer.Value.BytesReceived - peer.Value.BytesReceivedAtRechoke))
            .ToList();
        var decision = Choker.Decide(candidates, _options.UnchokeSlots, _optimistic, rotate, Random.Shared);

        if (rotate || !ReferenceEquals(decision.Optimistic, _optimistic))
            _lastOptimisticRotation = now;
        _optimistic = decision.Optimistic;
        _lastRechoke = now;

        foreach (var (peer, state) in _activePeers)
        {
            state.BytesReceivedAtRechoke = state.BytesReceived;
            SetChoking(peer, state, !decision.Unchoked.Contains(peer));
        }
    }

    private void SetChoking(PeerConnection peer, PeerState state, bool choking)
    {
        if (state.AmChoking == choking)
            return;

        state.AmChoking = choking;
        peer.SendChoke(choking);
        if (choking)
        {
            state.IncomingRequests.Clear();
            Log.PeerChoked(_logger, peer.Endpoint);
        }
        else
            Log.PeerUnchoked(_logger, peer.Endpoint, ReferenceEquals(peer, _optimistic));
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

        [LoggerMessage(2010, LogLevel.Information,
            "Peer {Endpoint} is snubbing us: no more blocks will be requested from it.")]
        public static partial void PeerSnubbed(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2011, LogLevel.Information, "Peer {Endpoint} is no longer snubbing us.")]
        public static partial void PeerUnsnubbed(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(2012, LogLevel.Warning,
            "Peer {Endpoint} has been snubbing us for more than {Timeout}. Disconnecting.")]
        public static partial void SnubTimedOut(ILogger logger, IPEndPoint endpoint, TimeSpan timeout);

        [LoggerMessage(2013, LogLevel.Debug, "Unchoking peer {Endpoint} (optimistic: {Optimistic}).")]
        public static partial void PeerUnchoked(ILogger logger, IPEndPoint endpoint, bool optimistic);

        [LoggerMessage(2014, LogLevel.Debug, "Choking peer {Endpoint}.")]
        public static partial void PeerChoked(ILogger logger, IPEndPoint endpoint);
    }
}
