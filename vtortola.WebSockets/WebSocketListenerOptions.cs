using System;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using JetBrains.Annotations;
using vtortola.WebSockets.Transports;

namespace vtortola.WebSockets
{
    [PublicAPI]
    public sealed class WebSocketListenerOptions
    {
        public const int DEFAULT_SEND_BUFFER_SIZE = 8 * 1024;
        public static readonly string[] NoSubProtocols = new string[0];

        [NotNull]
        public WebSocketTransportCollection Transports { get; private set; }

        [NotNull]
        public WebSocketFactoryCollection Standards { get; private set; }

        [NotNull]
        public WebSocketConnectionExtensionCollection ConnectionExtensions { get; private set; }

        public TimeSpan PingTimeout { get; set; }
        public TimeSpan PingInterval => this.PingTimeout > TimeSpan.Zero ? TimeSpan.FromTicks(this.PingTimeout.Ticks / 2) : TimeSpan.FromSeconds(5);

        public int NegotiationQueueCapacity { get; set; }
        public int ParallelNegotiations { get; set; }
        public TimeSpan NegotiationTimeout { get; set; }
        public int SendBufferSize { get; set; }
        public string[] SubProtocols { get; set; }
        public BufferManager BufferManager { get; set; }
        public HttpAuthenticationCallback HttpAuthenticationHandler { get; set; }
        public RemoteCertificateValidationCallback CertificateValidationHandler { get; set; }
        public SslProtocols SupportedSslProtocols { get; set; }
        public PingMode PingMode { get; set; }
        public IHttpFallback HttpFallback { get; set; }
        public ILogger Logger { get; set; }

        public WebSocketListenerOptions()
        {
            this.PingTimeout = TimeSpan.FromSeconds(5);
            this.Transports = new WebSocketTransportCollection();
            this.Standards = new WebSocketFactoryCollection();
            this.ConnectionExtensions = new WebSocketConnectionExtensionCollection();
            this.NegotiationQueueCapacity = Environment.ProcessorCount * 10;
            this.ParallelNegotiations = Environment.ProcessorCount * 2;
            this.NegotiationTimeout = TimeSpan.FromSeconds(5);
            this.SendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
            this.SubProtocols = NoSubProtocols;
            this.HttpAuthenticationHandler = null;
            this.CertificateValidationHandler = null;
            this.PingMode = PingMode.LatencyControl;
            this.SupportedSslProtocols = SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12;
#if DEBUG
            this.Logger = DebugLogger.Instance;
#else
            this.Logger = NullLogger.Instance;
#endif
        }

        public void CheckCoherence()
        {
            if (this.PingTimeout <= TimeSpan.Zero)
                this.PingTimeout = Timeout.InfiniteTimeSpan;

            if (this.PingTimeout <= TimeSpan.FromSeconds(1))
                this.PingTimeout = TimeSpan.FromSeconds(1);

            if (this.NegotiationQueueCapacity < 0)
                throw new WebSocketException($"{nameof(this.NegotiationQueueCapacity)} must be 0 or more. Actual value: {this.NegotiationQueueCapacity}");

            if (this.ParallelNegotiations < 1)
                throw new WebSocketException($"{nameof(this.ParallelNegotiations)} cannot be less than 1. Actual value: {this.ParallelNegotiations}");

            if (this.NegotiationTimeout == TimeSpan.Zero)
                this.NegotiationTimeout = Timeout.InfiniteTimeSpan;

            if (this.SendBufferSize < 1024)
                throw new WebSocketException($"{nameof(this.SendBufferSize)} must be bigger than 1024. Actual value: {this.SendBufferSize}");

            if (this.BufferManager != null && this.SendBufferSize < this.BufferManager.LargeBufferSize)
            {
                var sendBufferSizeName = nameof(this.SendBufferSize);

                throw new WebSocketException(
                    $"{this.BufferManager}.{this.BufferManager.LargeBufferSize} must be bigger or equals to {sendBufferSizeName}. Actual value of {sendBufferSizeName}: {this.SendBufferSize}");
            }

            if (this.Logger == null)
                throw new WebSocketException($"{this.Logger} should be set. You can use {nameof(NullLogger)}.{nameof(NullLogger.Instance)} to disable logging.");
        }

        public WebSocketListenerOptions Clone()
        {
            var cloned = (WebSocketListenerOptions)this.MemberwiseClone();
            cloned.SubProtocols = (string[])this.SubProtocols.Clone();
            cloned.Transports = this.Transports.Clone();
            cloned.Standards = this.Standards.Clone();
            cloned.ConnectionExtensions = this.ConnectionExtensions.Clone();
            return cloned;
        }
        public void SetUsed(bool isUsed)
        {
            this.Standards.SetUsed(isUsed);
            foreach (var standard in this.Standards)
                standard.MessageExtensions.SetUsed(true);
            this.ConnectionExtensions.SetUsed(isUsed);
            this.Transports.SetUsed(isUsed);
        }
    }
}
