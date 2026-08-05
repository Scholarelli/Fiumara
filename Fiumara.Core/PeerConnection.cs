using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Fiumara.Core;

public class PeerConnection(IPEndPoint endpoint, byte[] infoHash, string peerId, Action<PeerConnection> onDisconnected) : IDisposable
{
    private TcpClient? _tcpClient;
    private NetworkStream?_stream;
    private CancellationTokenSource? _cts;
    private readonly Action<PeerConnection> _onDisconnected = onDisconnected;

    private enum MessageType
    {
        KeepAlive = 0,
        Choke,
        Unchoke,
        Interested,
        NotInterested,
        Have
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

    public async Task ConnectAsync(CancellationToken externalToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        handshakeCts.CancelAfter(TimeSpan.FromSeconds(5));

        _tcpClient = new TcpClient();
        await _tcpClient.ConnectAsync(endpoint.Address, endpoint.Port, handshakeCts.Token);

        _stream = _tcpClient.GetStream();

        // Send handshake
        var handshakeMessage = CreateHandshakeMessage();
        await _stream.WriteAsync(handshakeMessage, handshakeCts.Token);

        // Read handshake response
        var responseBuffer = new byte[68];
        await _stream.ReadExactlyAsync(responseBuffer, handshakeCts.Token);

        // Validate handshake response
        if (!ValidateHandshakeResponse(responseBuffer))
        {
            throw new Exception("Invalid handshake response from peer.");
        }

        Console.WriteLine($"Connected to peer {endpoint.Address}:{endpoint.Port}");
        await ReceiveMessageAsync(_cts.Token);
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
            while(!cancellationToken.IsCancellationRequested && _stream is { CanRead: true })
            {
                await _stream.ReadExactlyAsync(lengthBuffer, cancellationToken);
                var messageLength = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);

                if (messageLength == 0) continue;
                var messageBuffer = new byte[messageLength];
                await _stream.ReadExactlyAsync(messageBuffer, cancellationToken);

                var messageId = messageBuffer[0];
                Console.WriteLine($"Received message of length {messageLength}: {BitConverter.ToString(messageBuffer)}");
            }
        } catch (Exception e)
        {
            Console.WriteLine(e);
        } finally{
            _onDisconnected(this);
        }
    }

    public void Disconnect()
    {
        _cts?.Cancel();
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _tcpClient?.Dispose();
        _stream?.Dispose();
    }
}