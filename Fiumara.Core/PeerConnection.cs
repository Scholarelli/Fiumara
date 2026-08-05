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
                    default:
                        Console.WriteLine($"Received message of type {messageId} with length {messageLength}");
                        break;

                }
            }
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

    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _tcpClient?.Dispose();
        _stream?.Dispose();
    }
}