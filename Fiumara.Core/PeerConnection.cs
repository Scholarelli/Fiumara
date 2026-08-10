using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Text;
using System.Threading.Channels;
using Fiumara.Core.Events;

namespace Fiumara.Core;

public class PeerConnection(IPEndPoint endpoint, byte[] infoHash, string peerId, ChannelWriter<PeerEvent> engineChannel) : IDisposable
{
    public IPEndPoint Endpoint => endpoint;
    private TcpClient? _tcpClient;
    private NetworkStream?_stream;
    private CancellationTokenSource? _cts;
    private readonly ChannelWriter<PeerEvent>? _engineChannel = engineChannel;
    public bool AmChoked { get; private set; } = true;

    public bool AmInterested { get; private set; } = false;
    public List<Block> PendingBlocks { get; } = [];

    private static readonly byte[] InterestedMessage = [0, 0, 0, 1, 2];

    private static readonly byte[] NotInterestedMessage = [0, 0, 0, 1, 3];
    private static readonly byte[] KeepAliveMessage = [0, 0, 0, 0];
    public async Task SetInterested(bool interested)
    {
        if (AmInterested == interested)
            return;

        AmInterested = interested;
        if (AmInterested)
            await SendMessageAsync(InterestedMessage);
        else
            await SendMessageAsync(NotInterestedMessage);
    }


    private enum MessageType : byte
    {
        Choke = 0,
        Unchoke = 1,
        Interested = 2,
        NotInterested = 3,
        Have = 4,
        Bitfield = 5,
        Request = 6,
        Piece = 7,
        Cancel = 8
    }
    public bool[] PeerBitfield { get; private set; } = [];

    public async Task ConnectAsync(CancellationToken externalToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);



        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(endpoint.Address, endpoint.Port, _cts.Token);

        _stream = _tcpClient.GetStream();

        await PerformHandshakeAsync(_cts.Token);

        Console.WriteLine($"Connected to peer {endpoint.Address}:{endpoint.Port}");
        _ = Task.Run(() => ReceiveMessageAsync(_cts.Token), _cts.Token);
    }

    private byte[] CreateHandshakeMessage()
    {
        const string protocolName = "BitTorrent protocol";
        var handshake = new byte[68];
        var span = handshake.AsSpan();
        span[0] = 19;
        span = span[1..];
        Encoding.ASCII.GetBytes(protocolName, span);
        span = span[19..];
        span = span[8..];
        infoHash.CopyTo(span);
        span = span[20..];
        Encoding.ASCII.GetBytes(peerId, span);

        return handshake;
    }

    private async Task PerformHandshakeAsync(CancellationToken cancellationToken)
    {
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        handshakeCts.CancelAfter(TimeSpan.FromSeconds(5));

        var handshakeMessage = CreateHandshakeMessage();
        await _stream!.WriteAsync(handshakeMessage, handshakeCts.Token);

        var responseBuffer = new byte[68];
        await _stream.ReadExactlyAsync(responseBuffer, handshakeCts.Token);


        if (!ValidateHandshakeResponse(responseBuffer))
        {
            throw new Exception("Invalid handshake response from peer.");
        }


    }

    private bool ValidateHandshakeResponse(byte[] response)
    {
        if(response[0] != 19)
            return false;


        var protocol = Encoding.ASCII.GetString(response, 1, 19);
        if (protocol != "BitTorrent protocol") return false;

        var responseHash = response.AsSpan(28, 20);
        return responseHash.SequenceEqual(infoHash);
    }


    public async Task ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        try
        {
            var lengthBuffer = new byte[4];
            while (!cancellationToken.IsCancellationRequested && _stream is { CanRead: true })
            {
                await _stream.ReadExactlyAsync(lengthBuffer, cancellationToken);
                var messageLength = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);

                if (messageLength == 0) continue;

                var messageBuffer = new byte[messageLength];
                await _stream.ReadExactlyAsync(messageBuffer, cancellationToken);

                var messageId = (MessageType)messageBuffer[0];
                var payload = messageBuffer.AsSpan(1);

                switch (messageId)
                {
                    case MessageType.Have:
                        ParseHave(payload);
                        break;
                    case MessageType.Bitfield:
                        ParseBitField(payload);
                        break;
                    case MessageType.Choke:
                        AmChoked = true;
                        _engineChannel!.TryWrite(new ChokeStateChanged(this, AmChoked));
                        break;
                    case MessageType.Unchoke:
                        AmChoked = false;
                        _engineChannel!.TryWrite(new ChokeStateChanged(this, AmChoked));
                        break;
                    case MessageType.Piece:
                        var index = BinaryPrimitives.ReadInt32BigEndian(payload[0..4]);
                        var offset = BinaryPrimitives.ReadInt32BigEndian(payload[4..8]);
                        payload = payload[8..];
                        _engineChannel!.TryWrite(new BlockReceived(this, index, offset, payload.ToArray()));
                        break;
                    default:
                        Console.WriteLine($"Received message of type {messageId} with length {messageLength}");
                        break;

                }
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

    private async Task SendMessageAsync(ReadOnlyMemory<byte> messageBuffer)
    {
        try
        {
            await _stream!.WriteAsync(messageBuffer, _cts!.Token);
        } catch  (Exception e)
        {
            Console.WriteLine(e);
        }
    }

    public async Task RequestBlockAsync(int pieceIndex, int offset, int size)
    {
        var requestMessage = new byte[17];
        var requestSpan = requestMessage.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(requestSpan[0..4], 13);
        requestMessage[4] = (byte)MessageType.Request;
        BinaryPrimitives.WriteInt32BigEndian(requestSpan[5..9], pieceIndex);
        BinaryPrimitives.WriteInt32BigEndian(requestSpan[9..13], offset);
        BinaryPrimitives.WriteInt32BigEndian(requestSpan[13..17], size);

        Console.WriteLine(" just Requested a block!");

        await SendMessageAsync(requestMessage);
    }

    public void Disconnect()
    {
        _cts?.Cancel();
    }

    private void ParseHave(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != 4)
        {
            Console.WriteLine("Invalid Have message length.");
            return;
        }

        int pieceIndex = BinaryPrimitives.ReadInt32BigEndian(payload);

        if (pieceIndex >= 0 && pieceIndex < PeerBitfield.Length)
        {
            if (PeerBitfield[pieceIndex]) return;
            PeerBitfield[pieceIndex] = true;
            Console.WriteLine($"Peer just acquired piece {pieceIndex}.");

            _engineChannel!.TryWrite(new HaveReceived(this, pieceIndex));
        }
        else
        {
            Console.WriteLine($"Peer sent out-of-bounds Have message: {pieceIndex}");
            Disconnect();
        }
    }

    private void ParseBitField(ReadOnlySpan<byte> payload)
    {
        PeerBitfield = new bool[payload.Length * 8];

        int piecesAvailable = 0;
        for (int i = 0; i < payload.Length; i++)
        {
            byte b = payload[i];

            piecesAvailable = BitOperations.PopCount(b);

            int baseIndex = i * 8;
            for(int bit = 0; bit < 8; bit++) {
                // We gotta adapt to big endian
                PeerBitfield[baseIndex + bit] = (b & (1 << (7 - bit))) != 0;

            }
        }


        _engineChannel!.TryWrite(new BitfieldReceived(this));
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _tcpClient?.Dispose();
        _stream?.Dispose();
    }
}