using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace vtortola.WebSockets
{
    [PublicAPI]
    public abstract class WebSocket : IDisposable
    {
        public abstract EndPoint RemoteEndpoint { get; }

        [NotNull]
        public WebSocketHttpRequest HttpRequest { get; }

        [NotNull]
        public WebSocketHttpResponse HttpResponse { get; }
        public abstract bool IsConnected { get; }
        public abstract EndPoint LocalEndpoint { get; }
        public abstract TimeSpan Latency { get; }
        public abstract string SubProtocol { get; }

        protected WebSocket([NotNull] WebSocketHttpRequest request, [NotNull] WebSocketHttpResponse response)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));
            if (response == null) throw new ArgumentNullException(nameof(response));

            this.HttpRequest = request;
            this.HttpResponse = response;
        }

        [NotNull, ItemCanBeNull]
        public abstract Task<WebSocketMessageReadStream> ReadMessageAsync(CancellationToken token);

        [NotNull]
        public abstract WebSocketMessageWriteStream CreateMessageWriter(WebSocketMessageType messageType);

        public Task SendPingAsync()
        {
            return this.SendPingAsync(null, 0, 0);
        }

        public abstract Task SendPingAsync(byte[] data, int offset, int count);
        public abstract Task CloseAsync();

        public abstract void Dispose();

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{this.GetType().Name}, remote: {this.RemoteEndpoint}, connected: {this.IsConnected}";
        }
    }
}
