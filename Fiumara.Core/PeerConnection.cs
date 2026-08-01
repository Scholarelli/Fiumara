using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Fiumara.Core;

public class PeerConnection(IPEndPoint endpoint, byte[] infoHash, string PeerId)
{
    private byte[] CreateHandshakeMessage()
    {
        const string protocolName = "BitTorrent protocol";
        var protocolNameLength = (byte)protocolName.Length;
        var reservedBytes = new byte[8]; // 8 reserved bytes set to 0

        var handshake = new byte[68];
        var span = handshake.AsSpan();
        span[0] = 19;
        span = span[1..];
        Encoding.ASCII.GetBytes(protocolName, span);
        span = span[19..];
        span = span[8..];
        infoHash.CopyTo(span);
        span = span[20..];
        Encoding.ASCII.GetBytes(PeerId, span);

        return handshake;
    }

    public async Task ConnectAsync()
    {
        using var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(endpoint.Address, endpoint.Port);

        await using var networkStream = tcpClient.GetStream();

        // Send handshake
        var handshakeMessage = CreateHandshakeMessage();
        await networkStream.WriteAsync(handshakeMessage);

        // Read handshake response
        var responseBuffer = new byte[68];
        await networkStream.ReadExactlyAsync(responseBuffer);

        // Validate handshake response
        if (!ValidateHandshakeResponse(responseBuffer))
        {
            throw new Exception("Invalid handshake response from peer.");
        }

        // Connection established, you can now send/receive messages
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
}