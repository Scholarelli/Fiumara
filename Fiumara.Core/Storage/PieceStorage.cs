using Microsoft.Win32.SafeHandles;

namespace Fiumara.Core.Storage;

// Where verified pieces end up on disk. Currently a single file, where piece i starts at i * pieceLength;
// multi-file torrents will map a piece onto the files it spans here, without the rest of the library noticing.
// Used only from the event loop.
internal sealed class PieceStorage(string path, long pieceLength) : IDisposable
{
    private SafeFileHandle? _handle;
    private bool _disposed;

    // The file is opened at the first write, so that building a TorrentHandle touches nothing on disk
    public async Task WritePieceAsync(int pieceIndex, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(pieceIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((long)data.Length, pieceLength, nameof(data));

        _handle ??= File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite,
            FileOptions.Asynchronous);

        // A positional write: no shared file position to Seek, so writes never depend on each other's order
        await RandomAccess.WriteAsync(_handle, data, pieceIndex * pieceLength, cancellationToken);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle?.Dispose();
    }
}
