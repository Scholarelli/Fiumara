using BencodeNET.Torrents;

namespace Fiumara.Core;

public class FiumaraEngine
{

    private static readonly string PeerId = $"-FMR0001-{Random.Shared.Next(1000000000):D11}";
    private readonly TrackerClient _trackerClient = new(PeerId);
    private TorrentInfo _torrent;

    private readonly List<PeerConnection> _activePeers = new();

    public async Task StartAsync(TorrentInfo torrentInfo)
    {
        _torrent = torrentInfo;
        var trackerResponse = await _trackerClient.GetPeersAsync(torrentInfo);
        var connectionTasks = new List<Task>();
        foreach (var endpoint in trackerResponse.Take(1))
        {
            var peer = new PeerConnection(endpoint, _torrent.InfoHash.ToArray(), PeerId);
            _activePeers.Add(peer);

            connectionTasks.Add(peer.ConnectAsync());
        }

        await Task.WhenAll(connectionTasks);


    }
}