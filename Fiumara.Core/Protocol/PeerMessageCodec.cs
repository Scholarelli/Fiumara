using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Resources;

namespace Fiumara.Core.Protocol;

internal static class PeerMessageCodec
{
    // a peer announcing more is broken or malicious, and must be refused before allocating anything
    internal const int MaxFrameLength = 1024 * 1024;

    internal const int HandshakeLength = 68;

    // Preceded on the wire by its own length, 19
    private static ReadOnlySpan<byte> ProtocolName => "BitTorrent protocol"u8;

    private const byte ChokeId = 0;
    private const byte UnchokeId = 1;
    private const byte InterestedId = 2;
    private const byte NotInterestedId = 3;
    private const byte HaveId = 4;
    private const byte BitfieldId = 5;
    private const byte RequestId = 6;
    private const byte PieceId = 7;
    private const byte CancelId = 8;

    private static readonly Dictionary<byte, int> FixedPayloadLength = new()
    {
        [ChokeId] = 0, [UnchokeId] = 0, [InterestedId] = 0, [NotInterestedId] = 0,
        [HaveId] = 4,
        [RequestId] = 12, [CancelId] = 12,
    };

    // The whole message, 4-byte length prefix included, ready to be written on the socket
    internal static byte[] Encode(PeerMessage message) => message switch
    {
        PeerMessage.KeepAlive => [0, 0, 0, 0],
        PeerMessage.Choke => Frame(ChokeId),
        PeerMessage.Unchoke => Frame(UnchokeId),
        PeerMessage.Interested => Frame(InterestedId),
        PeerMessage.NotInterested => Frame(NotInterestedId),
        PeerMessage.Have m => Frame(HaveId, [m.PieceIndex]),
        PeerMessage.Bitfield m => Frame(BitfieldId, [], m.Bits),
        PeerMessage.Request m => Frame(RequestId, [m.PieceIndex, m.Offset, m.Length]),
        PeerMessage.Piece m => Frame(PieceId, [m.PieceIndex, m.Offset], m.Data),
        PeerMessage.Cancel m => Frame(CancelId, [m.PieceIndex, m.Offset, m.Length]),
        _ => throw new ArgumentException($"{message.GetType().Name} cannot be encoded.", nameof(message))
    };

    // Decodes what follows the length prefix (id + payload)
    // Only the shape is checked: whether a piece index exists is for whoever knows the torrent.
    internal static bool TryDecode(ReadOnlySpan<byte> frame,
        [NotNullWhen(true)] out PeerMessage? message, [NotNullWhen(false)] out string? error)
    {
        message = null;
        error = null;

        if (frame.IsEmpty)
        {
            message = new PeerMessage.KeepAlive();
            return true;
        }

        byte id = frame[0];
        var payload = frame[1..];

        if (FixedPayloadLength.TryGetValue(id, out int expected) && payload.Length != expected)
        {
            error = $"Message {id} has a {payload.Length}-byte payload, expected {expected}.";
            return false;
        }

        if (id == PieceId && payload.Length < 8)
        {
            error = $"Piece message has a {payload.Length}-byte payload, expected at least 8.";
            return false;
        }

        // Protocol integers are unsigned: one that does not fit an int is malformed
        int fields = id switch { HaveId => 1, PieceId => 2, RequestId or CancelId => 3, _ => 0 };
        for (int i = 0; i < fields; i++)
        {
            if ((payload[i * 4] & 0x80) != 0)
            {
                error = $"Message {id} has a field larger than {int.MaxValue}.";
                return false;
            }
        }

        message = id switch
        {
            ChokeId => new PeerMessage.Choke(),
            UnchokeId => new PeerMessage.Unchoke(),
            InterestedId => new PeerMessage.Interested(),
            NotInterestedId => new PeerMessage.NotInterested(),
            HaveId => new PeerMessage.Have(ReadField(payload, 0)),
            BitfieldId => new PeerMessage.Bitfield(payload.ToArray()),
            RequestId => new PeerMessage.Request(ReadField(payload, 0), ReadField(payload, 1), ReadField(payload, 2)),
            PieceId => new PeerMessage.Piece(ReadField(payload, 0), ReadField(payload, 1), payload[8..].ToArray()),
            CancelId => new PeerMessage.Cancel(ReadField(payload, 0), ReadField(payload, 1), ReadField(payload, 2)),
            _ => new PeerMessage.Unknown(id)
        };
        return true;
    }

    // One bool per piece from a bitfield payload, checking its length and its padding against the torrent's piece count
    internal static bool TryExpandBitfield(byte[] bits, int pieceCount,
        [NotNullWhen(true)] out bool[]? pieces, [NotNullWhen(false)] out string? error)
    {
        pieces = null;
        error = null;

        // One bit per piece, rounded up to whole bytes: (n + 7) / 8 is an integer ceiling division
        int expectedLength = (pieceCount + 7) / 8;
        if (bits.Length != expectedLength)
        {
            error = $"Bitfield of {bits.Length} bytes, expected {expectedLength}.";
            return false;
        }

        // The bits after the last piece are padding and must be zero
        int spareBits = expectedLength * 8 - pieceCount;
        if (spareBits > 0 && (bits[^1] & ((1 << spareBits) - 1)) != 0)
        {
            error = "Bitfield has spare bits set.";
            return false;
        }

        pieces = new bool[pieceCount];
        for (int i = 0; i < pieceCount; i++)
        {
            // i >> 3 is the byte, i & 7 the bit inside it
            // the most significant bit is the lowest piece index
            pieces[i] = (bits[i >> 3] & (0x80 >> (i & 7))) != 0;
        }
        return true;
    }

    internal static byte[] EncodeHandshake(byte[] infoHash, byte[] peerId, ulong reserved = 0)
    {
        if (infoHash.Length != 20)
            throw new ArgumentException("The info hash must be 20 bytes.", nameof(infoHash));
        if (peerId.Length != 20)
            throw new ArgumentException("The peer id must be 20 bytes.", nameof(peerId));

        var bytes = new byte[HandshakeLength];
        bytes[0] = (byte)ProtocolName.Length;
        ProtocolName.CopyTo(bytes.AsSpan(1));
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(20), reserved);
        infoHash.CopyTo(bytes, 28);
        peerId.CopyTo(bytes, 48);
        return bytes;
    }

    // Checks the shape and that the peer answered for our torrent.
    // The peer id is not checked: compact tracker responses do not say which one to expect.
    internal static bool TryDecodeHandshake(ReadOnlySpan<byte> bytes, ReadOnlySpan<byte> expectedInfoHash,
        [NotNullWhen(false)] out string? error)
    {
        error = null;

        if (bytes.Length != HandshakeLength)
        {
            error = $"Handshake of {bytes.Length} bytes, expected {HandshakeLength}.";
            return false;
        }

        if (bytes[0] != ProtocolName.Length || !bytes.Slice(1, ProtocolName.Length).SequenceEqual(ProtocolName))
        {
            error = "Not a BitTorrent handshake.";
            return false;
        }

        var infoHash = bytes.Slice(28, 20);
        if (!infoHash.SequenceEqual(expectedInfoHash))
        {
            error = "The peer answered for a different torrent.";
            return false;
        }

        return true;
    }

    private static byte[] Frame(byte id, ReadOnlySpan<int> fields = default, ReadOnlySpan<byte> data = default)
    {
        int payloadLength = fields.Length * 4 + data.Length;
        var frame = new byte[5 + payloadLength];
        BinaryPrimitives.WriteInt32BigEndian(frame, 1 + payloadLength);
        frame[4] = id;
        for (int i = 0; i < fields.Length; i++)
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(5 + i * 4), fields[i]);
        data.CopyTo(frame.AsSpan(5 + fields.Length * 4));
        return frame;
    }

    private static int ReadField(ReadOnlySpan<byte> payload, int field) =>
        BinaryPrimitives.ReadInt32BigEndian(payload[(field * 4)..]);
}
