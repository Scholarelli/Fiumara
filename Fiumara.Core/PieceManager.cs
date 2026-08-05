using System.Net;

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

public class PieceInfo
{
    public int index {get; set;}
    public PieceState State {get; set;} = PieceState.Missing;
    public HashSet<IPEndPoint> peersWithPiece { get; } = [];

}

public class Piece
{
    public int Index { get; }
    public byte[] ExpectedHash { get; }

    public Block[] Blocks { get; private set; }

    public byte[]? Buffer {get; private set;}

    public Piece(int index, int pieceSize, byte[] expectedHash)
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
}

public class PieceManager
{
    private readonly PieceInfo[] _pieces;

    public PieceManager(int totalPieces)
    {
        _pieces = new PieceInfo[totalPieces];
        for (int i = 0; i < totalPieces; i++)
        {
            _pieces[i] = new PieceInfo { index = i };
        }
    }

    public void RegisterPeerPiece(IPEndPoint endPoint, bool[] peerBitField)
    {
        foreach (var peer in _pieces)
        {
            if (peerBitField[peer.index])
            {
                peer.peersWithPiece.Add(endPoint);
            }
        }

    }

    public void UnregisterPeerPiece(IPEndPoint endPoint, bool[] peerBitField)
    {
        foreach (var peer in _pieces)
        {
            if (peerBitField[peer.index])
                peer.peersWithPiece.Remove(endPoint);
        }
    }

    public PieceInfo? GetNextPieceToDownload(IPEndPoint unchokedPeer)
    {
        foreach (var piece in _pieces)
        {
            if (piece.State == PieceState.Missing && piece.peersWithPiece.Count > 0)
            {
                piece.State = PieceState.Pending;
                return piece;
            }
        }
        return null;
    }
}