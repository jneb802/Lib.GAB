using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Lib.GAB.Protocol;

// Unity/Mono compatibility note:
// Using dedicated threads instead of Task.Run for better compatibility
// with Unity's Mono runtime which has limited ThreadPool support.

namespace Lib.GAB.Transport
{
    /// <summary>
    /// TCP connection implementation
    /// </summary>
    public class TcpConnection : IConnection
    {
        internal readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly string _id;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private bool _disposed;

        public string Id => _id;
        public bool IsConnected => _client.Connected && !_disposed;

        public event EventHandler Disconnected;

        public TcpConnection(TcpClient client)
        {
            _client = client;
            _stream = client.GetStream();
            _id = Guid.NewGuid().ToString();
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task SendMessageAsync(GabpMessage message, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_disposed || !IsConnected)
                throw new InvalidOperationException("Connection is not active");

            var json = JsonConvert.SerializeObject(message);

            var jsonBytes = Encoding.UTF8.GetBytes(json);
            var header = $"Content-Length: {jsonBytes.Length}\r\nContent-Type: application/json\r\n\r\n";
            var headerBytes = Encoding.UTF8.GetBytes(header);

            var combinedToken = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token).Token;

            await _stream.WriteAsync(headerBytes, 0, headerBytes.Length, combinedToken);
            await _stream.WriteAsync(jsonBytes, 0, jsonBytes.Length, combinedToken);
            await _stream.FlushAsync(combinedToken);
        }

        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            _cancellationTokenSource.Cancel();
            
            try
            {
                Disconnected?.Invoke(this, EventArgs.Empty);
            }
            catch
            {
                // Ignore exceptions in event handlers
            }

            _stream?.Dispose();
            _client?.Close();
            _cancellationTokenSource?.Dispose();
        }
    }

    /// <summary>
    /// TCP transport implementation for GABP
    /// </summary>
    public class TcpTransport : ITransport
    {
        private readonly int _port;
        private TcpListener _listener;
        private bool _disposed;
        private bool _running;

        public event EventHandler<ConnectionEstablishedEventArgs> ConnectionEstablished;
        public event EventHandler<MessageReceivedEventArgs> MessageReceived;

        public TcpTransport(int port = 0)
        {
            _port = port;
        }

        public int Port { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_running)
                throw new InvalidOperationException("Transport is already running");

            _listener = new TcpListener(IPAddress.Loopback, _port);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _running = true;

            // Use a dedicated thread instead of Task.Run for Unity/Mono compatibility
            var acceptThread = new Thread(() => AcceptConnectionsLoop(cancellationToken))
            {
                IsBackground = true,
                Name = "GABP-Accept"
            };
            acceptThread.Start();
            
            return Task.FromResult(0);
        }

        public Task StopAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (!_running) return Task.FromResult(0);

            _running = false;
            _listener?.Stop();
            return Task.FromResult(0);
        }

        /// <summary>
        /// Synchronous accept loop for Unity/Mono compatibility
        /// </summary>
        private void AcceptConnectionsLoop(CancellationToken cancellationToken)
        {
            while (_running && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Use synchronous Accept for better Unity compatibility
                    if (!_listener.Pending())
                    {
                        Thread.Sleep(50); // Small sleep to prevent busy-waiting
                        continue;
                    }
                    
                    var tcpClient = _listener.AcceptTcpClient();
                    var connection = new TcpConnection(tcpClient);
                    
                    ConnectionEstablished?.Invoke(this, new ConnectionEstablishedEventArgs(connection));
                    
                    // Start a dedicated thread for reading messages from this connection
                    var readThread = new Thread(() => ReadMessagesLoop(connection, cancellationToken))
                    {
                        IsBackground = true,
                        Name = $"GABP-Read-{connection.Id.Substring(0, 8)}"
                    };
                    readThread.Start();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException)
                {
                    break;
                }
                catch (Exception)
                {
                    if (_running)
                    {
                        Thread.Sleep(1000);
                    }
                }
            }
        }

        /// <summary>
        /// Synchronous read loop for Unity/Mono compatibility
        /// </summary>
        private void ReadMessagesLoop(TcpConnection connection, CancellationToken cancellationToken)
        {
            var stream = connection._client.GetStream();
            var buffer = new byte[8192];
            var messageBuffer = new StringBuilder();

            try
            {
                // Set a read timeout so we can check cancellation periodically
                stream.ReadTimeout = 1000; // 1 second timeout
                
                while (connection.IsConnected && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Blocking read - will timeout after ReadTimeout ms
                        var bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead == 0)
                        {
                            break;
                        }

                        var data = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        messageBuffer.Append(data);

                        // Process complete messages
                        ProcessMessages(connection, messageBuffer);
                    }
                    catch (IOException)
                    {
                        // Read timeout - expected, continue to check cancellation
                        continue;
                    }
                }
            }
            catch (Exception)
            {
                // Connection lost or error occurred
            }
            finally
            {
                connection.Dispose();
            }
        }

        /// <summary>
        /// Synchronous message processing for Unity/Mono compatibility
        /// </summary>
        private void ProcessMessages(TcpConnection connection, StringBuilder buffer)
        {
            while (true)
            {
                var content = buffer.ToString();
                var headerEnd = content.IndexOf("\r\n\r\n", StringComparison.Ordinal);
                
                if (headerEnd == -1)
                {
                    break;
                }

                var headerText = content.Substring(0, headerEnd);
                var contentLengthIndex = headerText.IndexOf("Content-Length:", StringComparison.OrdinalIgnoreCase);
                
                if (contentLengthIndex == -1)
                {
                    break;
                }

                var startIndex = contentLengthIndex + "Content-Length:".Length;
                var endIndex = headerText.IndexOf('\r', startIndex);
                if (endIndex == -1) endIndex = headerText.IndexOf('\n', startIndex);
                // If no newline after the value, use end of header text
                if (endIndex == -1) endIndex = headerText.Length;

                var contentLengthStr = headerText.Substring(startIndex, endIndex - startIndex).Trim();
                if (!int.TryParse(contentLengthStr, out var contentLength))
                {
                    break;
                }

                var messageStart = headerEnd + 4;
                if (content.Length < messageStart + contentLength)
                {
                    break;
                }

                var messageJson = content.Substring(messageStart, contentLength);
                
                try
                {
                    var message = ParseMessage(messageJson);
                    if (message != null)
                    {
                        MessageReceived?.Invoke(this, new MessageReceivedEventArgs(connection, message));
                    }
                }
                catch (Exception)
                {
                    // Failed to parse message
                }

                // Remove processed message from buffer
                buffer.Remove(0, messageStart + contentLength);
            }
        }

        private static GabpMessage ParseMessage(string json)
        {
            var messageDict = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
            if (messageDict == null || !messageDict.ContainsKey("type"))
                return null;

            var type = messageDict["type"].ToString();
            switch (type)
            {
                case "request":
                    return JsonConvert.DeserializeObject<GabpRequest>(json);
                case "response":
                    return JsonConvert.DeserializeObject<GabpResponse>(json);
                case "event":
                    return JsonConvert.DeserializeObject<GabpEvent>(json);
                default:
                    return null;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_running)
            {
                _running = false;
                _listener?.Stop();
            }
        }
    }
}
