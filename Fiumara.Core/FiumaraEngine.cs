using System.Collections.Concurrent;
using System.Net;
using System.Reflection.Metadata;
using System.Threading.Channels;
using BencodeNET.Torrents;
using Fiumara.Core.Events;

namespace Fiumara.Core;

public class FiumaraEngine
{
    private readonly Channel<PeerEvent> _eventChannel = Channel.CreateUnbounded<PeerEvent>();
    private PieceManager _pieceManager;

    private static readonly string PeerId = $"-FMR0001-{Random.Shared.Next(1000000000):D11}";
    private readonly TrackerClient _trackerClient = new(PeerId);
    private TorrentInfo _torrent;

    private readonly ConcurrentQueue<IPEndPoint> _waitingRoom = [];
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _activePeers = [];

    private Task ConnectAsync(int targetCount)
    {
        for (int i = 0; i < targetCount; i++)
        {
            if (!_waitingRoom.TryDequeue(out var endpoint))
            {
                Console.WriteLine("No more peers available in the waiting room.");
                break;
            }

            var peer = new PeerConnection(endpoint, _torrent.InfoHash.ToArray(), PeerId, _eventChannel.Writer);
            _activePeers.TryAdd(endpoint, peer);

            _ = SafeConnectAsync(peer, endpoint);
        }

        return Task.CompletedTask;
    }

    private async Task SafeConnectAsync(PeerConnection peer, IPEndPoint endpoint)
    {
        try
        {
            await peer.ConnectAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Peer {endpoint}] Connection failed: {ex.Message}");

            // Let the engine clean up the dead peer and optionally spawn a replacement
            _eventChannel.Writer.TryWrite(new PeerDisconnected(peer));
        }
    }

    public async Task StartAsync(TorrentInfo torrentInfo)
    {
        _torrent = torrentInfo;
        _pieceManager = new PieceManager(_torrent);
        var trackerResponse = await _trackerClient.GetPeersAsync(torrentInfo);
        trackerResponse.ForEach(endpoint => _waitingRoom.Enqueue(endpoint));
        _ = Task.Run(ProcessEventsAsync);

        _ = ConnectAsync(5);
    }

    private void HandlePeerDisconnect(PeerConnection deadPeer, IPEndPoint endPoint)
    {
        deadPeer.Dispose();
        _activePeers.TryRemove(endPoint, out var _);

        if (_waitingRoom.Count > 0)
        {
            var endpoint = _waitingRoom.TryDequeue(out var ep) ? ep : null;
            if (endpoint == null) return;
            var newPeer = new PeerConnection(endpoint, _torrent.InfoHash.ToArray(), PeerId, _eventChannel.Writer);
            _activePeers.TryAdd(endpoint, newPeer);
            newPeer.ConnectAsync().ContinueWith(t =>
            {
                if (!t.IsFaulted) return;
                Console.Error.WriteLine($"Failed to connect to peer {endpoint}: {t.Exception?.GetBaseException().Message}");
                HandlePeerDisconnect(newPeer, endPoint);
            });
        }
    }

    private async Task ProcessEventsAsync()
    {
        await foreach (var peerEvent in _eventChannel.Reader.ReadAllAsync())
        {
            switch (peerEvent)
            {
                case ChokeStateChanged c:
                    await EvaluatePeerAsync(c.Peer);
                    break;
                case BitfieldReceived b:
                    Console.WriteLine("Just got bitfield");
                    _pieceManager.RegisterPeerPiece(b.Peer.Endpoint, b.Peer.PeerBitfield);
                    break;
                case HaveReceived h:
                    _pieceManager.RegisterPeerPiece(h.Peer.Endpoint, h.PieceIndex);
                    break;
                case PeerDisconnected d:
                    HandlePeerDisconnect(d.Peer, d.Peer.Endpoint);
                    break;
                case BlockReceived b:
                    Console.WriteLine("Just got a block omg");
                    break;
            }
        }
    }

    private async Task EvaluatePeerAsync(PeerConnection peer)
    {

        var targetPiece = _pieceManager.GetNextPieceToDownload(peer.Endpoint);

        var interest = targetPiece != null;

        await peer.SetInterested(interest);
    }

    private async Task FillPeerPipelineAsync(PeerConnection peer)
    {
        if (peer.AmChoked)
            return;

        int maxPipelineSize = 0;

        while (peer.PendingRequests < maxPipelineSize)
        {
            var targetPiece = _pieceManager.GetNextPieceToDownload(peer.Endpoint);
            if (targetPiece == null) break;

            if(targetPiece.Buffer == null)
            {
                int pieceSize = (int)targetPiece.Blocks.Sum(b => b.Size);
                targetPiece.AllocateBuffer(pieceSize);
            }

            var block = targetPiece.GetNextMissingBlock();
            if (block == null) break;

            peer.PendingRequests++;
            await peer.RequestBlockAsync(targetPiece.Index, (int)block.Offset, (int)block.Size);

        }
    }
}