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
    private FileStream _fileStream;

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
        _fileStream = new FileStream("download.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
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
                    Console.WriteLine($"Choke state changed: {c.Peer.Endpoint}:  {c.Peer.AmChoked}");
                    if (c.Peer.AmChoked)
                    {
                        foreach (var block in c.Peer.PendingBlocks.Where(block => block.State == State.Pending))
                        {
                            block.State = State.Missing;
                        }
                        c.Peer.PendingBlocks.Clear();
                        break;
                    }
                    await EvaluatePeerAsync(c.Peer);
                    await FillPeerPipelineAsync(c.Peer);
                    break;
                case BitfieldReceived b:
                    Console.WriteLine("Just got bitfield");
                    _pieceManager.RegisterPeerPiece(b.Peer.Endpoint, b.Peer.PeerBitfield);
                    await EvaluatePeerAsync(b.Peer);
                    break;
                case HaveReceived h:
                    _pieceManager.RegisterPeerPiece(h.Peer.Endpoint, h.PieceIndex);
                    await EvaluatePeerAsync(h.Peer);
                    break;
                case PeerDisconnected d:
                    HandlePeerDisconnect(d.Peer, d.Peer.Endpoint);
                    break;
                case BlockReceived b:
                    var piece = _pieceManager.GetPiece(b.PieceIndex);


                    piece!.SaveBlock(b.Offset, b.Data);

                    var pending = b.Peer.PendingBlocks.FirstOrDefault(x => x.Offset == b.Offset);
                    if (pending != null) b.Peer.PendingBlocks.Remove(pending);
                    if (!piece.IsComplete()) continue;
                    if (piece.VerifyHash())
                    {
                        Console.WriteLine($"[SUCCESS] Piece {piece.Index} verified! Writing to disk...");
                        long writePosition = (long)piece.Index * _torrent.PieceLength;
                        _fileStream.Seek(writePosition, SeekOrigin.Begin);
                        await _fileStream.WriteAsync(piece.Buffer);

                        // TODO: Send HAVE Message to all peers!
                    }
                    else
                    {
                        Console.Error.WriteLine($"[ERROR] Piece {piece.Index} failed hash check! Trashing data.");
                        piece.Reset();
                    }

                    Console.WriteLine($"[Peer {b.Peer.Endpoint}] Just got a block omg");
                    Console.WriteLine(
                        $"[Peer {b.Peer.Endpoint}] Received block for piece {b.PieceIndex} at offset {b.Offset}, size {b.Data.Length}");
                    await FillPeerPipelineAsync(b.Peer);
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

        int maxPipelineSize = 3;

        while (peer.PendingBlocks.Count < maxPipelineSize)
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
            else
            {
                peer.PendingBlocks.Add(block);


                await peer.RequestBlockAsync(targetPiece.Index, (int)block.Offset, (int)block.Size);

            }

            Console.WriteLine(
                $"[Peer {peer.Endpoint}] Requesting block {block}, at offset {block.Offset}, {block.Size}",
                peer.Endpoint);

        }
    }
}