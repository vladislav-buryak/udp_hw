using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp7;
class Program
{
    private static readonly object ClientsLock = new();
    private static readonly List<ClientState> Clients = new();

    private static int Secret;
    private static int GameOver = 0;

    static async Task Main()
    {
        Secret = new Random().Next(1, 101);
        Console.WriteLine($"[SERVER] Secret number chosen (1..100).");

        int port = 5002;
        var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"[SERVER] Listening on {port}...");

        try
        {
            while (Volatile.Read(ref GameOver) == 0)
            {
                var tcp = await listener.AcceptTcpClientAsync();
                var state = new ClientState(tcp);

                lock (ClientsLock) Clients.Add(state);

                Console.WriteLine($"[SERVER] Connected: {state.Remote}");

                _ = Task.Run(() => HandleClientAsync(state));
            }
        }
        catch { /* listener stopped */ }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task HandleClientAsync(ClientState c)
    {
        try
        {
            using (c.Tcp)
            using (var stream = c.Tcp.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true))
            using (var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true) { AutoFlush = true })
            {
                c.Writer = writer;

                // Поки гра не закінчена — читаємо спроби
                while (Volatile.Read(ref GameOver) == 0)
                {
                    string? line = await reader.ReadLineAsync();
                    if (line == null) break;

                    if (!line.StartsWith("GUESS|"))
                    {
                        await SafeWriteAsync(c, "ERROR");
                        continue;
                    }

                    string numPart = line.Substring("GUESS|".Length).Trim();
                    if (!int.TryParse(numPart, out int guess))
                    {
                        await SafeWriteAsync(c, "ERROR");
                        continue;
                    }

                    if (guess < 1 || guess > 100)
                    {
                        await SafeWriteAsync(c, "ERROR");
                        continue;
                    }

                    if (guess < Secret)
                    {
                        await SafeWriteAsync(c, "LOW");
                    }
                    else if (guess > Secret)
                    {
                        await SafeWriteAsync(c, "HIGH");
                    }
                    else
                    {
                        // Хтось вгадав
                        await SafeWriteAsync(c, "CORRECT");

                        if (Interlocked.CompareExchange(ref GameOver, 1, 0) == 0)
                        {
                            string winMsg = $"Гравець із {c.Remote} переміг!";
                            Console.WriteLine("[SERVER] " + winMsg);

                            await BroadcastAsync("WIN|" + winMsg);
                            await ShutdownAsync();
                            Environment.Exit(0); // завершуємо роботу сервера
                        }
                        break;
                    }
                }
            }
        }
        catch
        {
            // клієнт міг відвалитися
        }
        finally
        {
            RemoveClient(c);
            Console.WriteLine($"[SERVER] Disconnected: {c.Remote}");
        }
    }

    private static async Task SafeWriteAsync(ClientState c, string line)
    {
        try
        {
            if (c.Writer == null) return;
            // захист від паралельних записів у того ж клієнта
            lock (c.WriteLock)
            {
                c.Writer.WriteLine(line);
            }
            await Task.CompletedTask;
        }
        catch { }
    }

    private static async Task BroadcastAsync(string line)
    {
        List<ClientState> snapshot;
        lock (ClientsLock) snapshot = new List<ClientState>(Clients);

        foreach (var c in snapshot)
            await SafeWriteAsync(c, line);
    }

    private static async Task ShutdownAsync()
    {
        List<ClientState> snapshot;
        lock (ClientsLock) snapshot = new List<ClientState>(Clients);

        foreach (var c in snapshot)
        {
            try { c.Tcp.Close(); } catch { }
        }

        await Task.CompletedTask;
    }

    private static void RemoveClient(ClientState c)
    {
        lock (ClientsLock)
        {
            Clients.Remove(c);
        }
    }

    private class ClientState
    {
        public TcpClient Tcp { get; }
        public StreamWriter? Writer { get; set; }
        public object WriteLock { get; } = new object();
        public string Remote { get; }

        public ClientState(TcpClient tcp)
        {
            Tcp = tcp;
            Remote = tcp.Client.RemoteEndPoint?.ToString() ?? "unknown";
        }
    }
}
