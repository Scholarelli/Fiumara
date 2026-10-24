using System.Net;
using System.Web;
using BencodeNET.Objects;
using BencodeNET.Parsing;

namespace Fiumara.Core;

public class TrackerClient(string PeerId, int port = 6881)
{
    private static readonly BencodeParser Parser = new();
    private readonly HttpClient _httpClient = new();
    private readonly string _peerid = PeerId;
    private readonly string _port = port.ToString();

    public async Task<List<IPEndPoint>>GetPeersAsync(TorrentInfo parsed)
    {
        return ParsePeers(await GetTrackerResponseAsync(parsed));
    }

    private async Task<byte[]> GetTrackerResponseAsync(TorrentInfo parsed)
    {
        var queryParams = new Dictionary<string, string>
        {
            ["info_hash"] = HttpUtility.UrlEncode(parsed.InfoHash.ToArray()), // Encoded exactly once
            ["peer_id"] = _peerid,
            ["port"] = _port,
            ["uploaded"] = "0",
            ["downloaded"] = "0",
            ["left"] = parsed.TotalSize.ToString(),
            ["compact"] = "1"
        };

        var uri = new UriBuilder(parsed.Announce);
        uri.Query = string.Join("&", queryParams.Select(kvp => $"{kvp.Key}={kvp.Value}"));
        var response = await _httpClient.GetAsync(uri.Uri);

        response.EnsureSuccessStatusCode();

        return await response.Content.ReadAsByteArrayAsync();
    }

    private List<IPEndPoint> ParsePeers(byte[] response)
    {
        var dict = Parser.Parse<BDictionary>(response);

        if(dict.TryGetValue("failure reason", out var value))
            throw new Exception($"Tracker failure: {value}");

        var interval = TimeSpan.FromSeconds(((BNumber)dict["interval"]).Value);
        var peersBString = ((BString)dict["peers"]!).Value;


        var endpoints = new List<IPEndPoint>();

        for(int i = 0; i < peersBString.Length; i += 6)
        {
            var ipBytes = peersBString.Span.Slice(i, 4);
            var portBytes = peersBString.Span.Slice(i + 4, 2);

            var ipAddress = new IPAddress(ipBytes);
            var port = (portBytes[0] << 8) | portBytes[1];

            endpoints.Add(new IPEndPoint(ipAddress, port));
        }
        return endpoints;
    }
}