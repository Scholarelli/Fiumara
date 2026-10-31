using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Fiumara.Core.Events;
using Fiumara.Core.Protocol;
using Microsoft.Extensions.Logging;

namespace Fiumara.Core;

public partial class PeerConnection(IPEndPoint endpoint, byte[] infoHash, string peerId, ChannelWriter<EngineEvent> engineChannel, int piecesCount, ILogger<PeerConnection> logger) : IDisposable
{
    public IPEndPoint Endpoint => endpoint;
    private TcpClient? _tcpClient;
    private NetworkStream?_stream;
    private CancellationTokenSource? _cts;
    private readonly ChannelWriter<EngineEvent>? _engineChannel = engineChannel;
    private readonly ILogger<PeerConnection> _logger = logger;
    private readonly Channel<PeerMessage> _outbox =
        Channel.CreateUnbounded<PeerMessage>(new UnboundedChannelOptions { SingleReader = true });

    public async Task ConnectAsync(TimeSpan handshakeTimeout, CancellationToken externalToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);



        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(endpoint.Address, endpoint.Port, _cts.Token);

        _stream = _tcpClient.GetStream();

        var remotePeerId = await PerformHandshakeAsync(handshakeTimeout, _cts.Token);

        Log.Connected(_logger, endpoint);
        var token = _cts.Token;
        _engineChannel!.TryWrite(new PeerConnected(this, remotePeerId));
        _ = Task.Run(() => ReceiveMessageAsync(token));
        _ = Task.Run(() => WriteLoopAsync(token), token);
    }

    private async Task<byte[]> PerformHandshakeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        handshakeCts.CancelAfter(timeout);

        await _stream!.WriteAsync(PeerMessageCodec.EncodeHandshake(infoHash, Encoding.ASCII.GetBytes(peerId)),
            handshakeCts.Token);

        var responseBuffer = new byte[PeerMessageCodec.HandshakeLength];
        await _stream.ReadExactlyAsync(responseBuffer, handshakeCts.Token);

        if (!PeerMessageCodec.TryDecodeHandshake(responseBuffer, infoHash, out var remotePeerId, out var error))
            throw new InvalidDataException($"Invalid handshake from peer: {error}");

        return remotePeerId;
    }

    private async Task ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        try
        {
            var lengthBuffer = new byte[4];
            var receivedAnyMessage = false;
            while (!cancellationToken.IsCancellationRequested && _stream is { CanRead: true })
            {
                await _stream.ReadExactlyAsync(lengthBuffer, cancellationToken);
                var messageLength = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
                switch (messageLength)
                {
                    case 0:
                        continue;
                    case > PeerMessageCodec.MaxFrameLength:
                        Log.FrameTooLarge(_logger, endpoint, messageLength, PeerMessageCodec.MaxFrameLength);
                        Disconnect();
                        return;
                }

                var messageBuffer = new byte[messageLength];
                await _stream.ReadExactlyAsync(messageBuffer, cancellationToken);

                if (!PeerMessageCodec.TryDecode(messageBuffer, out var decodedMessage, out var error))
                {
                    Log.MalformedMessage(_logger, endpoint, error);
                    Disconnect();
                    return;
                }

                if (decodedMessage is PeerMessage.Bitfield && receivedAnyMessage)
                {
                    Log.LateBitfield(_logger, endpoint);
                    Disconnect();
                    return;
                }
                receivedAnyMessage = true;

                EngineEvent? engineEvent = decodedMessage switch
                {
                    PeerMessage.Choke => new ChokeStateChanged(this, true),
                    PeerMessage.Unchoke => new ChokeStateChanged(this, false),
                    PeerMessage.Interested => new InterestedStateChanged(this, true),
                    PeerMessage.NotInterested => new InterestedStateChanged(this, false),
                    PeerMessage.Have have => OnHave(have.PieceIndex),
                    PeerMessage.Bitfield bitfield => OnBitfield(bitfield.Bits),
                    PeerMessage.Piece piece => new BlockReceived(this, piece.PieceIndex, piece.Offset, piece.Data),
                    PeerMessage.KeepAlive or PeerMessage.Unknown or PeerMessage.Request or PeerMessage.Cancel => null,
                    _ => throw new UnreachableException($"Unhandled message {decodedMessage}")
                };

                if (engineEvent is not null)
                    _engineChannel!.TryWrite(engineEvent);
            }
        }
        catch (EndOfStreamException)
        {
            Log.ClosedByPeer(_logger, endpoint);
        }
        catch (IOException e)
        {
            Log.ConnectionLost(_logger, endpoint, e.Message);
        }
        catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException)
        {
        }
        catch (Exception e)
        {
            Log.ReceiveFailed(_logger, endpoint, e);
        }
        finally
        {
            _engineChannel!.TryWrite(new PeerDisconnected(this));
        }
    }

    private EngineEvent? OnBitfield(byte[] bits)
    {
        if (!PeerMessageCodec.TryExpandBitfield(bits, piecesCount, out var pieces, out var error))
        {
            Log.InvalidBitfield(_logger, endpoint, error);
            Disconnect();
            return null;
        }

        return new BitfieldReceived(this, pieces);
    }

    private EngineEvent? OnHave(int pieceIndex)
    {
        // The codec already guarantees pieceIndex >= 0: only the upper bound depends on the torrent
        if (pieceIndex >= piecesCount)
        {
            Log.HaveOutOfRange(_logger, endpoint, pieceIndex, piecesCount);
            Disconnect();
            return null;
        }

        return new HaveReceived(this, pieceIndex);
    }
    public void SendHave(int pieceIndex) => Send(new PeerMessage.Have(pieceIndex));

    public void RequestBlock(int pieceIndex, int offset, int size) =>
        Send(new PeerMessage.Request(pieceIndex, offset, size));

    public void SendInterested(bool interested) =>
        Send(interested ? new PeerMessage.Interested() : new PeerMessage.NotInterested());

    private void Send(PeerMessage message) => _outbox.Writer.TryWrite(message);

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _outbox.Reader.ReadAllAsync(cancellationToken))
                await _stream!.WriteAsync(PeerMessageCodec.Encode(message), cancellationToken);
        }
        catch (Exception e) when (e is OperationCanceledException or ObjectDisposedException)
        {
        }
        catch (Exception e)
        {
            Log.WriteFailed(_logger, endpoint, e.Message);
            Disconnect();
        }
    }

    private int _disposed;
    public void Disconnect()
    {
        if(Volatile.Read(ref _disposed) == 1)
            return;
        _outbox.Writer.TryComplete();
        try { _cts?.Cancel(); }
        catch (ObjectDisposedException) { }

    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _outbox.Writer.TryComplete();
        _cts?.Cancel();
        _cts?.Dispose();
        _tcpClient?.Dispose();
        _stream?.Dispose();
    }

    private static partial class Log
    {
        [LoggerMessage(3001, LogLevel.Information, "Connected to peer {Endpoint}.")]
        public static partial void Connected(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(3002, LogLevel.Warning,
            "Peer {Endpoint} announced a {Length}-byte message, more than the {MaxLength} allowed. Disconnecting.")]
        public static partial void FrameTooLarge(ILogger logger, IPEndPoint endpoint, uint length, int maxLength);

        [LoggerMessage(3003, LogLevel.Warning, "Peer {Endpoint} sent a malformed message: {Error} Disconnecting.")]
        public static partial void MalformedMessage(ILogger logger, IPEndPoint endpoint, string error);

        [LoggerMessage(3004, LogLevel.Warning, "Peer {Endpoint} sent a bitfield after other messages. Disconnecting.")]
        public static partial void LateBitfield(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(3005, LogLevel.Information, "Peer {Endpoint} closed the connection.")]
        public static partial void ClosedByPeer(ILogger logger, IPEndPoint endpoint);

        [LoggerMessage(3006, LogLevel.Information, "Lost the connection to peer {Endpoint}: {Reason}")]
        public static partial void ConnectionLost(ILogger logger, IPEndPoint endpoint, string reason);

        [LoggerMessage(3007, LogLevel.Error, "Receiving from peer {Endpoint} failed.")]
        public static partial void ReceiveFailed(ILogger logger, IPEndPoint endpoint, Exception exception);

        [LoggerMessage(3008, LogLevel.Warning, "Peer {Endpoint} sent an invalid bitfield: {Error} Disconnecting.")]
        public static partial void InvalidBitfield(ILogger logger, IPEndPoint endpoint, string error);

        [LoggerMessage(3009, LogLevel.Warning,
            "Peer {Endpoint} announced piece {PieceIndex}, but the torrent has {PieceCount} pieces. Disconnecting.")]
        public static partial void HaveOutOfRange(ILogger logger, IPEndPoint endpoint, int pieceIndex, int pieceCount);

        [LoggerMessage(3010, LogLevel.Information, "Writing to peer {Endpoint} failed: {Reason}")]
        public static partial void WriteFailed(ILogger logger, IPEndPoint endpoint, string reason);
    }
}