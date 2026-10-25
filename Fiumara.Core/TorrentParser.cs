namespace Fiumara.Core;

using BencodeNET.Parsing;
using BencodeNET.Torrents;


public readonly record struct TorrentInfo(
    string Name,
    IReadOnlyList<Uri> AnnounceList,
    bool IsSingleFile,
    long TotalSize,
    IReadOnlyList<byte> InfoHash,
    long PieceLength,
    IReadOnlyList<byte> Pieces)
{
    public Uri Announce => AnnounceList[0];
    public int PieceCount => (int)((TotalSize + PieceLength - 1) / PieceLength);
}

public class TorrentParser
{

    private static readonly BencodeParser Parser = new();


    public TorrentInfo? Parse(ReadOnlySpan<byte> data)
    {
        using var stream = new MemoryStream(data.ToArray());
        var result = Parser.Parse<Torrent>(stream);
        if (result == null) return null;

        // For now we flatten the list for simplicity
        var rawTrackers = result.Trackers?.SelectMany(t => t).ToList() ?? [];

        if(rawTrackers.Count == 0) return null;

        var announceUris = new List<Uri>();
        foreach (var tracker in rawTrackers)
        {
            if (Uri.TryCreate(tracker, UriKind.Absolute, out var uri))
                announceUris.Add(uri);
        }

        if (announceUris.Count == 0) return null;

        return new TorrentInfo(
            result.DisplayName,
            announceUris,
            result.FileMode is TorrentFileMode.Single,
            result.TotalSize,
            result.OriginalInfoHashBytes,
            result.PieceSize,
            result.Pieces ?? []
        );
    }
}