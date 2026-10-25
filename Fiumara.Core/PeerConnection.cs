using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Fiumara.Core.Events;
using Fiumara.Core.Protocol;

namespace Fiumara.Core;

public class PeerConnection(IPEndPoint endpoint, byte[] infoHash, string peerId, ChannelWriter<EngineEvent> engineChannel, int piecesCount) : IDisposable
{
    public IPEndPoint Endpoint => endpoint;
    private TcpClient? _tcpClient;
    private NetworkStream?_stream;
    private CancellationTokenSource? _cts;
    private readonly ChannelWriter<EngineEvent>? _engineChannel = engineChannel;
    private readonly Channel<PeerMessage> _outbox =
        Channel.CreateUnbounded<PeerMessage>(new UnboundedChannelOptions { SingleReader = true });

    public async Task ConnectAsync(TimeSpan handshakeTimeout, CancellationToken externalToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);



        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(endpoint.Address, endpoint.Port, _cts.Token);

        _stream = _tcpClient.GetStream();

        await PerformHandshakeAsync(handshakeTimeout, _cts.Token);

        Console.WriteLine($"Connected to peer {endpoint.Address}:{endpoint.Port}");
        _ = Task.Run(() => ReceiveMessageAsync(_cts.Token), _cts.Token);
        _ = Task.Run(() => WriteLoopAsync(_cts.Token), _cts.Token);
    }

    private async Task PerformHandshakeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        handshakeCts.CancelAfter(timeout);

        await _stream!.WriteAsync(PeerMessageCodec.EncodeHandshake(infoHash, Encoding.ASCII.GetBytes(peerId)),
            handshakeCts.Token);

        var responseBuffer = new byte[PeerMessageCodec.HandshakeLength];
        await _stream.ReadExactlyAsync(responseBuffer, handshakeCts.Token);

        if (!PeerMessageCodec.TryDecodeHandshake(responseBuffer, infoHash, out var error))
            throw new InvalidDataException($"Invalid handshake from peer: {error}");


    }

    private async Task ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        try
        {
            var lengthBuffer = new byte[4];
            while (!cancellationToken.IsCancellationRequested && _stream is { CanRead: true })
            {
                await _stream.ReadExactlyAsync(lengthBuffer, cancellationToken);
                var messageLength = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
                switch (messageLength)
                {
                    case 0:
                        continue;
                    case > PeerMessageCodec.MaxFrameLength:
                        Console.WriteLine($"Peer {endpoint.Address}:{endpoint.Port} sent a message larger than the maximum allowed length of {PeerMessageCodec.MaxFrameLength} bytes. Disconnecting.");
                        Disconnect();
                        return;
                }

                var messageBuffer = new byte[messageLength];
                await _stream.ReadExactlyAsync(messageBuffer, cancellationToken);

                var decoded = PeerMessageCodec.TryDecode(messageBuffer, out var decodedMessage, out var error);
                if(!decoded)
                {
                    Console.WriteLine($"Failed to decode message from peer {endpoint.Address}:{endpoint.Port}: {error}");
                    Disconnect();
                    return;
                }

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
            Console.WriteLine($"[Peer disconnected] {endpoint.Address}:{endpoint.Port} closed the stream");
        }
        catch (IOException e)
        {
            Console.WriteLine($"[Peer Dropped] {Endpoint} connection lost: {e.Message}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
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
            Console.WriteLine($"[{Endpoint}] Invalid bitfield: {error} Disconnecting.");
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
            Console.WriteLine($"[{Endpoint}] Have for piece {pieceIndex}, but the torrent has {piecesCount} pieces. Disconnecting.");
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
            Console.WriteLine($"[{Endpoint}] Write failed: {e.Message}");
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
}