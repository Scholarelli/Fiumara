using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Fiumara.Core.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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

    private readonly ILogger<FiumaraEngine> _logger;
    private Task? _eventLoop;

    public Task Completion => _eventLoop
                              ?? throw new InvalidOperationException("FiumaraEngine has not been initialized.");

    public FiumaraEngine(ILoggerFactory? loggerFactory = null)
    {
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<FiumaraEngine>();
    }

    private Task ConnectAsync(int targetCount)
    {
        for (int i = 0; i < targetCount; i++)
        {
            if (!_waitingRoom.TryDequeue(out var endpoint))
            {
                _logger.LogInformation("No more peers available in the waiting room.");
                break;
            }

            var peer = new PeerConnection(endpoint, [.. _torrent.InfoHash], PeerId, _eventChannel.Writer,
                (int)Math.Ceiling((double)_torrent.TotalSize / _torrent.PieceLength));
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
            _logger.LogError("Attempted to connect to peer {Endpoint} but failed: {ErrorMessage}", endpoint,
                ex.Message);

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

        _eventLoop = Task.Run(RunEventLoopAsync);


        _ = ConnectAsync(15);
    }

    private async Task RunEventLoopAsync()
    {
        try
        {
            await ProcessEventsAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Event loop crashed");
            throw;
        }
    }

    private void HandlePeerDisconnect(PeerConnection deadPeer, IPEndPoint endPoint)
    {
        deadPeer.Dispose();
        _pieceManager.UnregisterPeerPiece(endPoint, deadPeer.PeerBitfield);
        _activePeers.TryRemove(endPoint, out var _);

        if (_waitingRoom.IsEmpty) return;
        _ = ConnectAsync(1);
    }

    private async Task HandleBlockReceived(BlockReceived b)
    {
        var piece = _pieceManager.GetPiece(b.PieceIndex);

        piece!.SaveBlock(b.Offset, b.Data);

        var pending = b.Peer.PendingBlocks.FirstOrDefault(x => x.Offset == b.Offset);
        if (pending != null) b.Peer.PendingBlocks.Remove(pending);

        _logger.LogInformation(
            "[{Endpoint}][INFO] Received block for piece {PieceIndex} at offset {Offset}, size {DataLength}",
            b.Peer.Endpoint, b.PieceIndex, b.Offset, b.Data.Length);
        if (!piece.IsComplete()) return;
        if (piece.VerifyHash())
        {
            _logger.LogInformation("[{Endpoint}][SUCCESS] Piece {PieceIndex} verified! Writing to disk...",
                b.Peer.Endpoint, piece.Index);
            long writePosition = (long)piece.Index * _torrent.PieceLength;
            _fileStream.Seek(writePosition, SeekOrigin.Begin);
            await _fileStream.WriteAsync(piece.Buffer);

            foreach (var peer in _activePeers.Values)
            {
                if (peer.PeerBitfield[piece.Index]) continue;
                await peer.SendHaveAsync(piece.Index);
                _logger.LogInformation(
                    "[{Endpoint}][INFO] Sent HAVE message for piece {PieceIndex} to peer {PeerEndpoint}",
                    b.Peer.Endpoint, piece.Index, peer.Endpoint);
            }
        }
        else
        {
            _logger.LogError("[{Endpoint}][ERROR] Piece {PieceIndex} failed hash check! Trashing data.",
                b.Peer.Endpoint, piece.Index);
            piece.Reset();
        }
    }


    private async Task ProcessEventsAsync()
    {
        await foreach (var peerEvent in _eventChannel.Reader.ReadAllAsync())
        {
            switch (peerEvent)
            {
                case ChokeStateChanged c:
                    _logger.LogInformation("[{Endpoint}][CHOKE] Choke state: {State}", c.Peer.Endpoint,
                        c.Peer.AmChoked ? "Chocked" : "Unchoked");
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
                    await HandleBlockReceived(b);
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

            if (targetPiece.Buffer == null)
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

            _logger.LogInformation(
                "[{Endpoint}][INFO] Requesting block {Block} from piece {Piece}, at offset {Offset}, {Size}",
                peer.Endpoint, block, targetPiece.Index, block.Offset, block.Size);
        }
    }
}