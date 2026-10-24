using System.Net;
using System.Security.Cryptography;
using BencodeNET.Torrents;

namespace Fiumara.Core;

public enum State {Missing, Completed}

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

public readonly record struct BlockRequest(int PieceIndex, int Offset, int Length);

public enum BlockOutcome
{
    Rejected,       // invalid piece index, offset or length: the peer is misbehaving
    Duplicate,      // the block was already completed
    Stored,         // saved, but the piece is not complete yet
    PieceVerified,  // the piece is complete and its hash matches
    PieceCorrupted  // the piece is complete but its hash is wrong: its blocks are missing again
}

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

    public void AllocateBuffer()
    {
        Buffer ??= new byte[(int)Blocks.Sum(b => b.Size)];
    }

    public bool IsComplete() => Blocks.All(b => b.State == State.Completed);

    public int Rarity => AvailableOnPeers.Count;

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
}

public class PieceManager
{
    private readonly Piece[] _pieces;
    private readonly RequestsRegister _requests;

    public PieceManager(TorrentInfo torrent, RequestsRegister? requests = null)
    {
        _requests = requests ?? new RequestsRegister();

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

    public Piece GetPiece(int index)
    {
        return _pieces[index];
    }

    public Piece? GetNextPieceToDownload(IPEndPoint unchokedPeer)
    {

        // Gotta finish what we started man
        var partialPieces = _pieces
            .FirstOrDefault(p => IsPartial(p) && p.AvailableOnPeers.Contains(unchokedPeer));
        if(partialPieces != null) return partialPieces;

        var candidatePieces = _pieces.Where(p =>
            !p.IsComplete() &&
            !IsFullyRequested(p) &&
            p.AvailableOnPeers.Contains(unchokedPeer));


        var rarestPiece = candidatePieces
            .OrderBy(p => p.Rarity)
            .Take(10)
            .Shuffle()
            .FirstOrDefault();

        return rarestPiece;
    }

    public int CountPendingRequests(PeerConnection peerConnection) =>
        _requests.CountPeerPending(peerConnection);

    // Choosing the block and registering the request is a single atomic unit
    public BlockRequest? ReserveNextBlock(PeerConnection peerConnection)
    {
        var piece = GetNextPieceToDownload(peerConnection.Endpoint);
        if (piece == null) return null;

        var block = NextFreeBlock(piece);
        if (block == null) return null;

        if (!_requests.TryAdd(block, peerConnection))
            throw new InvalidOperationException(
                $"Block at offset {block.Offset} of piece {piece.Index} was chosen while already requested.");

        piece.AllocateBuffer();
        return new BlockRequest(piece.Index, (int)block.Offset, (int)block.Size);
    }

    public BlockOutcome OnBlockReceived(int pieceIndex, int offset, byte[] data)
    {
        if (pieceIndex < 0 || pieceIndex >= _pieces.Length) return BlockOutcome.Rejected;

        var piece = _pieces[pieceIndex];
        var block = piece.Blocks.FirstOrDefault(b => b.Offset == offset);
        if (block == null || data.Length != block.Size) return BlockOutcome.Rejected;

        // Closes the request whoever it was assigned to: a late answer carries good data too
        _requests.TryRemove(block, out _);
        if (block.State == State.Completed) return BlockOutcome.Duplicate;

        piece.AllocateBuffer();
        piece.SaveBlock(offset, data);
        if (!piece.IsComplete()) return BlockOutcome.Stored;
        if (piece.VerifyHash()) return BlockOutcome.PieceVerified;

        piece.Reset();
        return BlockOutcome.PieceCorrupted;
    }

    public IReadOnlyList<Block> ReleasePeerRequests(PeerConnection peerConnection) =>
        _requests.RemovePeerPending(peerConnection);

    private bool IsFullyRequested(Piece piece) =>
        piece.Blocks.All(b => b.State == State.Completed || _requests.IsPending(b));

    private bool IsPartial(Piece piece) =>
        piece.Blocks.Any(b => b.State == State.Completed || _requests.IsPending(b)) && !IsFullyRequested(piece);

    private Block? NextFreeBlock(Piece piece) =>
        piece.Blocks.FirstOrDefault(b => b.State == State.Missing && !_requests.IsPending(b));
}
