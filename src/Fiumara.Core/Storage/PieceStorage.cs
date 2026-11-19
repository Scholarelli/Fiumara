using Microsoft.Win32.SafeHandles;

namespace Fiumara.Core.Storage;

// Where verified pieces end up on disk. Currently a single file, where piece i starts at i * pieceLength;
// multi-file torrents will map a piece onto the files it spans here, without the rest of the library noticing.
// Used only from the event loop.
internal sealed class PieceStorage(string path, long pieceLength) : IDisposable
{
    private SafeFileHandle? _handle;
    private bool _disposed;

    // The file is opened at the first write or read, so that building a TorrentHandle touches nothing on disk
    public async Task WritePieceAsync(int pieceIndex, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((long)data.Length, pieceLength, nameof(data));

        // A positional write: no shared file position to Seek, so writes never depend on each other's order
        await RandomAccess.WriteAsync(Handle, data, pieceIndex * pieceLength, cancellationToken);
    }

    public async Task<byte[]> ReadBlockAsync(int pieceIndex, int offset, int length,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((long)offset + length, pieceLength, nameof(length));

        var block = new byte[length];
        var position = pieceIndex * pieceLength + offset;
        var read = 0;
        while (read < length)
        {
            var count = await RandomAccess.ReadAsync(Handle, block.AsMemory(read), position + read, cancellationToken);
            if (count == 0)
                throw new EndOfStreamException($"The file ends inside the block at offset {offset} of piece {pieceIndex}.");
            read += count;
        }

        return block;
    }

    private SafeFileHandle Handle => _handle ??= File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite,
        FileShare.ReadWrite, FileOptions.Asynchronous);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle?.Dispose();
    }
}
