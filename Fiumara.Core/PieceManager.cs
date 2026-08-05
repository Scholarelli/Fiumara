using System.Net;
using BencodeNET.Torrents;

namespace Fiumara.Core;

public enum State {Missing, Pending, Completed}

public class Block
{
    public uint Offset { get; set; }
    public uint Size { get; set; }
    public State State { get; set; } = State.Missing;
}

public enum PieceState
{
    Missing,
    Pending,
    Completed
};

public class Piece
{
    public int Index { get; }
    public byte[] ExpectedHash { get; }

    public Block[] Blocks { get; private set; }

    public byte[]? Buffer {get; private set;}

    public HashSet<IPEndPoint> AvailableOnPeers { get; } = new();

    public Piece(int index, long pieceSize, byte[] expectedHash)
    {
        Index = index;
        ExpectedHash = expectedHash;
        const int blockSize = 16 * 1024;
        int blockCount = (int)Math.Ceiling((double) pieceSize / blockSize);

        Blocks = new Block[blockCount];
        for(int i = 0 ; i < blockCount; i++)
        {
            uint offset = (uint)(i * blockSize);
            uint size = (uint)Math.Min(blockSize, pieceSize - offset);
            Blocks[i] = new Block { Offset = offset, Size = size };
        }
    }

    public void AllocateBuffer(int pieceSize)
    {
        Buffer = new  byte[pieceSize];
    }

    public bool IsComplete() => Blocks.All(b => b.State == State.Completed);
    public bool IsFullyRequested() => Blocks.All(b => b.State != State.Missing);

    public int Rarity => AvailableOnPeers.Count;
}

public class PieceManager
{
    private readonly Piece[] _pieces;

    public PieceManager(TorrentInfo torrent)
    {
        int totalPieces = (int)Math.Ceiling((double)torrent.TotalSize / torrent.PieceLength);
        _pieces = new Piece[totalPieces];

        ReadOnlySpan<byte> allHashes = torrent.Pieces.ToArray();
        for (int i = 0; i < totalPieces; i++)
        {
            byte[] expectedHash = allHashes.Slice(i * 20, 20).ToArray();

            var remainingBytes = torrent.TotalSize - (i * torrent.PieceLength);
            var actualPieceSize = Math.Min(torrent.PieceLength, remainingBytes);
            _pieces[i] = new Piece(i,
                actualPieceSize,
                expectedHash);
        }
    }

    public void RegisterPeerPiece(IPEndPoint endPoint, bool[] peerBitField)
    {
        foreach (var peer in _pieces)
        {
            if (peerBitField[peer.Index])
            {
                peer.AvailableOnPeers.Add(endPoint);
            }
        }

    }

    public void UnregisterPeerPiece(IPEndPoint endPoint, bool[] peerBitField)
    {
        foreach (var peer in _pieces)
        {
            if (peerBitField[peer.Index])
                peer.AvailableOnPeers.Remove(endPoint);
        }
    }

    public Piece? GetNextPieceToDownload(IPEndPoint unchokedPeer)
    {
        foreach (var piece in _pieces)
        {
            if (!piece.IsComplete() && piece.AvailableOnPeers.Count > 0)
            {

                return piece;
            }
        }
        return null;
    }
}