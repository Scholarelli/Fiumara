namespace Fiumara.Core.Protocol;

// What peers say on the wire (BEP 3). Messages are plain data: turning them into bytes and back is PeerMessageCodec's job.
// The types are nested so that names like Piece do not clash with the download's own Piece class.
internal abstract record PeerMessage
{
    internal sealed record KeepAlive : PeerMessage;
    internal sealed record Choke : PeerMessage;
    internal sealed record Unchoke : PeerMessage;
    internal sealed record Interested : PeerMessage;
    internal sealed record NotInterested : PeerMessage;
    internal sealed record Have(int PieceIndex) : PeerMessage;
    internal sealed record Bitfield(byte[] Bits) : PeerMessage;
    internal sealed record Request(int PieceIndex, int Offset, int Length) : PeerMessage;
    internal sealed record Piece(int PieceIndex, int Offset, byte[] Data) : PeerMessage;
    internal sealed record Cancel(int PieceIndex, int Offset, int Length) : PeerMessage;

    // An id this client does not implement (e.g. extensions it never announced)
    internal sealed record Unknown(byte Id) : PeerMessage;
}
