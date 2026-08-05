using System.Collections.Concurrent;
using System.Net;
using BencodeNET.Torrents;

namespace Fiumara.Core;

public class FiumaraEngine
{

    private static readonly string PeerId = $"-FMR0001-{Random.Shared.Next(1000000000):D11}";
    private readonly TrackerClient _trackerClient = new(PeerId);
    private TorrentInfo _torrent;

    private readonly ConcurrentQueue<IPEndPoint> _waitingRoom = [];
    private readonly ConcurrentDictionary<IPEndPoint, PeerConnection> _activePeers = [];

    private async Task ConnectAsync(int newPeersCount)
    {
        var connectionTasks = new List<Task>();
        for (int i = 0; i < newPeersCount; i++)
        {
            var endpoint = _waitingRoom.TryDequeue(out var ep) ? ep : null;
            if (endpoint == null)
            {
                Console.WriteLine("Could not get  peer");
                break;
            };
            var peer = new PeerConnection(endpoint, _torrent.InfoHash.ToArray(), PeerId,
                connection => HandlePeerDisconnect(connection, endpoint));
            _activePeers.TryAdd(endpoint, peer);
            connectionTasks.Add(peer.ConnectAsync());
        }

        await Task.WhenAll(connectionTasks);
    }

    public async Task StartAsync(TorrentInfo torrentInfo)
    {
        _torrent = torrentInfo;
        var trackerResponse = await _trackerClient.GetPeersAsync(torrentInfo);
        trackerResponse.ForEach(endpoint => _waitingRoom.Enqueue(endpoint));

        await ConnectAsync(5); // Start with one peer for now
        await DownloadAsync();
    }

    private void HandlePeerDisconnect(PeerConnection deadPeer, IPEndPoint endPoint)
    {
        deadPeer.Dispose();
        _activePeers.TryRemove(endPoint, out var _);

        if (_waitingRoom.Count > 0)
        {
            var endpoint = _waitingRoom.TryDequeue(out var ep) ? ep : null;
            if (endpoint == null) return;
            var newPeer = new PeerConnection(endpoint, _torrent.InfoHash.ToArray(), PeerId, connection => HandlePeerDisconnect(connection, endpoint));
            _activePeers.TryAdd(endpoint, newPeer);
            newPeer.ConnectAsync().ContinueWith(t =>
            {
                if (!t.IsFaulted) return;
                Console.Error.WriteLine($"Failed to connect to peer {endpoint}: {t.Exception?.GetBaseException().Message}");
                HandlePeerDisconnect(newPeer, endPoint);
            });
        }
    }

    public async Task DownloadAsync()
    {
         bool isDownloaded = false; // we are leechers for now
         while (!isDownloaded)
         {
             foreach (var peer in _activePeers.Values)
             {

             }
         }
    }
}