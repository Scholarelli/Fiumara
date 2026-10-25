using System.Threading.Channels;
using Fiumara.Core.Events;
using Fiumara.Core.Protocol;
using Fiumara.Core.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Fiumara.Core;

public class TorrentHandle
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
        _swarm = new PeerSwarm(_torrent.InfoHash, _torrent.PieceCount, _eventChannel.Writer, _options,
            loggers.CreateLogger<PeerSwarm>());

        _timeProvider = timeProvider ?? TimeProvider.System;
        _trackerClient = new TrackerClient(PeerSwarm.PeerId, _options.ListenPort);

        _pieceManager = new PieceManager(_torrent, new RequestsRegister(_options.RequestTimeout, _timeProvider));
        _storage = new PieceStorage(_options.DownloadPath, _torrent.PieceLength);

    }


    public async Task StartAsync()
    {
        var trackerResponse = await _trackerClient.GetPeersAsync(_torrent);
        trackerResponse.ForEach(endpoint => _swarm.AddEndpoint(endpoint));

        _eventLoop = Task.Run(RunEventLoopAsync);
        _ticker = RunTickerAsync();
        _swarm.ConnectUpTo(_options.MaxPeers);
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
            _logger.LogError(ex, "Ticker crashed");
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
            _logger.LogError(ex, "Event loop crashed");
            throw;
        }
    }

    private void HandlePeerDisconnect(PeerConnection deadPeer)
    {
        _pieceManager.ReleasePeerRequests(deadPeer);
        _pieceManager.UnregisterPeerPiece(deadPeer.Endpoint);
        _swarm.OnPeerDisconnected(deadPeer);
    }

    private async Task HandleBlockReceived(BlockReceived b)
    {
        var outcome = _pieceManager.OnBlockReceived(b.PieceIndex, b.Offset, b.Data);

        _logger.LogDebug(
            "[{Endpoint}][INFO] Received block for piece {PieceIndex} at offset {Offset}, size {DataLength}: {Outcome}",
            b.Peer.Endpoint, b.PieceIndex, b.Offset, b.Data.Length, outcome);

        switch (outcome)
        {
            case BlockOutcome.PieceVerified:
                var piece = _pieceManager.GetPiece(b.PieceIndex);
                _logger.LogInformation("[{Endpoint}][SUCCESS] Piece {PieceIndex} verified! Writing to disk...",
                    b.Peer.Endpoint, piece.Index);
                await _storage.WritePieceAsync(piece.Index, piece.Buffer);
                _swarm.BroadcastHave(piece.Index);
                break;
            case BlockOutcome.PieceCorrupted:
                _logger.LogError("[{Endpoint}][ERROR] Piece {PieceIndex} failed hash check! Trashing data.",
                    b.Peer.Endpoint, b.PieceIndex);
                break;
            case BlockOutcome.Rejected:
                _logger.LogWarning(
                    "[{Endpoint}][WARN] Invalid block (piece {PieceIndex}, offset {Offset}, size {DataLength}). Disconnecting.",
                    b.Peer.Endpoint, b.PieceIndex, b.Offset, b.Data.Length);
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
                    _logger.LogDebug("[{Endpoint}] Ignoring {Event} from an inactive peer.", peerEvent.Peer.Endpoint, peerEvent.GetType().Name);
                    break;
            }
        }
    }

    private void HandleTick()
    {
        _pieceManager.RemoveExpiredRequests();
        foreach (var (peer, state) in _swarm.ActivePeers)
        {
            EvaluatePeer(peer);
            FillPeerPipeline(peer, state);
        }
    }

    private async Task HandlePeerEventAsync(PeerEvent peerEvent, PeerState state)
    {
        switch (peerEvent)
        {
            case ChokeStateChanged c:
                state.PeerChoking = c.IsChoked;
                _logger.LogInformation("[{Endpoint}][CHOKE] Choke state: {State}", c.Peer.Endpoint,
                    c.IsChoked ? "Choked" : "Unchoked");
                if (c.IsChoked)
                {
                    _pieceManager.ReleasePeerRequests(c.Peer);
                    break;
                }
                EvaluatePeer(c.Peer);
                FillPeerPipeline(c.Peer, state);
                break;
            case BitfieldReceived b:
                _pieceManager.RegisterPeerPiece(b.Peer.Endpoint, b.BitField);
                state.Has = b.BitField;
                EvaluatePeer(b.Peer);
                break;
            case HaveReceived h:
                _pieceManager.RegisterPeerPiece(h.Peer.Endpoint, h.PieceIndex);
                state.Has[h.PieceIndex] = true;
                EvaluatePeer(h.Peer);
                break;
            case PeerDisconnected d:
                HandlePeerDisconnect(d.Peer);
                break;
            case BlockReceived b:
                await HandleBlockReceived(b);
                FillPeerPipeline(b.Peer, state);
                break;
            case InterestedStateChanged i:
                state.PeerInterested = i.IsInterested;
                _logger.LogInformation("[{Endpoint}][INTEREST] Peer interest state: {State}",
                    i.Peer.Endpoint, i.IsInterested ? "Interested" : "Not Interested");
                break;
            default:
                _logger.LogWarning("[{Endpoint}] Unhandled event type: {EventType}", peerEvent.Peer.Endpoint,
                    peerEvent.GetType().Name);
                break;
        }
    }

    private void EvaluatePeer(PeerConnection peer) =>
        _swarm.SetInterest(peer, _pieceManager.GetNextPieceToDownload(peer.Endpoint) != null);

    private void FillPeerPipeline(PeerConnection peer, PeerState state)
    {
        if (state.PeerChoking)
            return;

        int maxPipelineSize = _options.MaxPipelineSize;

        while (_pieceManager.CountPendingRequests(peer) < maxPipelineSize)
        {
            if (_pieceManager.ReserveNextBlock(peer) is not { } request) break;

            peer.RequestBlock(request.PieceIndex, request.Offset, request.Length);

            _logger.LogDebug(
                "[{Endpoint}][INFO] Requesting block from piece {Piece}, at offset {Offset}, {Size}",
                peer.Endpoint, request.PieceIndex, request.Offset, request.Length);
        }
    }
}