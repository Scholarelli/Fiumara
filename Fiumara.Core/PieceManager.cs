using System.Net;
using System.Security.Cryptography;
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
        Buffer ??= new  byte[pieceSize];
    }

    public bool IsComplete() => Blocks.All(b => b.State == State.Completed);
    public bool IsFullyRequested() => Blocks.All(b => b.State != State.Missing);

    public int Rarity => AvailableOnPeers.Count;

    public Block? GetNextMissingBlock()
    {
        var block = Blocks.FirstOrDefault(b => b.State == State.Missing);
        block?.State = State.Pending;
        return block;
    }

    public void SaveBlock(int offset, byte[] data)
    {
        if (Buffer == null) return;
        System.Buffer.BlockCopy(data, 0, Buffer, offset, data.Length);
        var block = Blocks.FirstOrDefault(b => b.Offset == offset);
        block?.State = State.Completed;
    }

    public bool VerifyHash()
    {
        if (Buffer == null)
            return false;

        byte[] actualHash = SHA1.HashData(Buffer);

        return actualHash.SequenceEqual(ExpectedHash);
    }

    public void Reset()
    {
        foreach (var block in Blocks)
        {
            block.State = State.Missing;
        }
    }

    public bool isPartial() => Blocks.Any(b => b.State != State.Missing) && !IsFullyRequested();
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
        foreach (var piece in _pieces)
        {
            if (peerBitField[piece.Index])
            {
                piece.AvailableOnPeers.Add(endPoint);
            }
        }

    }

    public void RegisterPeerPiece(IPEndPoint endPoint, int pieceIndex)
    {
        if (pieceIndex >= 0 && pieceIndex < _pieces.Length)
        {
            _pieces[pieceIndex].AvailableOnPeers.Add(endPoint);
        }
    }

    public void UnregisterPeerPiece(IPEndPoint endPoint, bool[] peerBitField)
    {
        foreach (var piece in _pieces)
        {
            if (peerBitField[piece.Index])
                piece.AvailableOnPeers.Remove(endPoint);
        }
    }

    public Piece? GetPiece(int index)
    {
        return _pieces[index];
    }

    public Piece? GetNextPieceToDownload(IPEndPoint unchokedPeer)
    {

        // Gotta finish what we started man
        var partialPieces = _pieces
            .FirstOrDefault(p => p.isPartial() && p.AvailableOnPeers.Contains(unchokedPeer));
        if(partialPieces != null) return partialPieces;

        var candidatePieces = _pieces.Where(p =>
            !p.IsComplete()&&
            !p.IsFullyRequested() &&
            p.AvailableOnPeers.Contains(unchokedPeer));

        var rarestPiece = candidatePieces
            .OrderBy(p => p.Rarity)
            .Take(10)
            .Shuffle()
            .FirstOrDefault();

        return rarestPiece;
    }
}