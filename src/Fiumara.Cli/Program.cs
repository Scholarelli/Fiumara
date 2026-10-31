using System.Web;
using Fiumara.Core;
using Microsoft.Extensions.Logging;

namespace Fiumara.Cli;
class Program
{
    static async Task<int> Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("Usage: Fiumara.Cli <torrent-file>");
            return 1;
        }

        var bytes = File.ReadAllBytes(args[0]).AsSpan();

        var parser = new Fiumara.Core.TorrentParser();
        var torrent = parser.Parse(bytes)!;
        if (torrent is not {} parsed)
        {
            Console.Error.WriteLine("Failed to parse torrent file.");
            return 1;
        }

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });

        var engine = new TorrentHandle(parsed, loggerFactory);
        try
        {
            await engine.StartAsync();
            await engine.Completion;
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Fiumara stopped: {ex.Message}");
            return 1;
        }
    }
}