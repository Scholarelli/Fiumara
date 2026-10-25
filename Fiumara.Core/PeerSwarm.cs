using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
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
}

internal sealed record ActivePeer(PeerConnection Connection, PeerState State);

internal class PeerSwarm
{
    public static readonly string PeerId = $"-FMR0001-{Random.Shared.Next(1000000000):D11}";
    private readonly ConcurrentQueue<IPEndPoint> _waitingRoom = [];
    private readonly ConcurrentDictionary<IPEndPoint, ActivePeer> _activePeers = [];
    private readonly ISwarmOptions _options;
    private readonly ChannelWriter<EngineEvent> _eventChannel;
    private readonly IReadOnlyList<byte> _infoHash;
    private readonly int _pieceCount;
    private readonly ILogger<PeerSwarm> _logger;


    internal PeerSwarm(IReadOnlyList<byte> infoHash, int pieceCount, ChannelWriter<EngineEvent> events,
        ISwarmOptions options, ILogger<PeerSwarm> logger)
    {
        _options = options;
        _logger = logger;
        _eventChannel = events;
        _infoHash = infoHash;
        _pieceCount = pieceCount;
    }

    public IReadOnlyCollection<ActivePeer> ActivePeers => _activePeers.Values.ToList();
    public int ActivePeerCount => _activePeers.Count;

    public bool TryGetState(PeerConnection peer, [NotNullWhen(true)] out PeerState? state)
    {
        if (_activePeers.TryGetValue(peer.Endpoint, out var active) && ReferenceEquals(active.Connection, peer))
        {
            state = active.State;
            return true;
        }

        state = null;
        return false;
    }

    private void RemovePeer(PeerConnection connection)
    {
        if (_activePeers.TryGetValue(connection.Endpoint, out var active) && ReferenceEquals(active.Connection,
                                                                              connection)
                                                                          && _activePeers.TryRemove(
                                                                              new KeyValuePair<IPEndPoint, ActivePeer>(
                                                                                  connection.Endpoint, active)))
            _logger.LogInformation("Peer {Endpoint} removed from active peers.", connection.Endpoint);
        else
            _logger.LogInformation("Peer {Endpoint} was not found in active peers.", connection.Endpoint);
    }

    public void AddEndpoint(IPEndPoint endpoint)
    {
        if (_activePeers.ContainsKey(endpoint))
        {
            _logger.LogInformation("Peer {Endpoint} is already connected.", endpoint);
            return;
        }

        if (_waitingRoom.Contains(endpoint))
        {
            _logger.LogInformation("Peer {Endpoint} is already in the waiting room.", endpoint);
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
                _logger.LogInformation("No more peers available in the waiting room.");
                break;
            }


            var peer = new PeerConnection(endpoint, [.. _infoHash], PeerId, _eventChannel, _pieceCount);
            _activePeers.TryAdd(endpoint, new ActivePeer(peer, new PeerState(_pieceCount)));

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
            _logger.LogError("Attempted to connect to peer {Endpoint} but failed: {ErrorMessage}", endpoint,
                ex.Message);

            // Let the handle clean up the dead peer and optionally spawn a replacement
            _eventChannel.TryWrite(new PeerDisconnected(peer));
        }
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
        foreach (var (peer, state) in _activePeers.Values)
        {
            if (state.Has[pieceIndex]) continue;
            peer.SendHave(pieceIndex);
            sent++;
        }

        _logger.LogDebug("Sent HAVE for piece {PieceIndex} to {Count} peers.", pieceIndex, sent);
    }

    public void SetInterest(PeerConnection peer, bool interested)
    {
        if (!TryGetState(peer, out var state) || state.AmInterested == interested)
            return;

        state.AmInterested = interested;
        peer.SendInterested(interested);
    }
}