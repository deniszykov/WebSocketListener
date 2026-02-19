/*
	Copyright (c) 2017 Denis Zykov
	License: https://opensource.org/licenses/MIT
*/
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using vtortola.WebSockets.Tools;
using vtortola.WebSockets.Transports.Sockets;

namespace vtortola.WebSockets.Transports.Tcp
{
    public sealed class TcpTransport : SocketTransport
    {
        private const int DEFAULT_HTTP_PORT = 80;
        private const int DEFAULT_SECURE_HTTP_PORT = 443;

        public const int DEFAULT_SEND_BUFFER_SIZE = 1024;
        public const int DEFAULT_RECEIVE_BUFFER_SIZE = 1024;
        public const int DEFAULT_SEND_TIMEOUT_MS = 5000;
        public const int DEFAULT_RECEIVE_TIMEOUT_MS = 5000;
        public const bool DEFAULT_NO_DELAY = false;
        public const bool DEFAULT_IS_ASYNC = true;
        public const bool DEFAULT_DUAL_MODE = false;
#if !NETSTANDARD && !UAP
        public const IPProtectionLevel DEFAULT_IP_PROTECTION_LEVEL = IPProtectionLevel.Unspecified;
#endif

        private static readonly string[] SupportedSchemes = { "tcp", "ws", "wss" };

        public LingerOption LingerState { get; set; }
        public bool NoDelay { get; set; } = DEFAULT_NO_DELAY;
        public int ReceiveBufferSize { get; set; } = DEFAULT_RECEIVE_BUFFER_SIZE;
        public TimeSpan ReceiveTimeout { get; set; } = TimeSpan.FromMilliseconds(DEFAULT_RECEIVE_TIMEOUT_MS);
        public int SendBufferSize { get; set; } = DEFAULT_SEND_BUFFER_SIZE;
        public TimeSpan SendTimeout { get; set; } = TimeSpan.FromMilliseconds(DEFAULT_SEND_TIMEOUT_MS);
        public bool DualMode { get; set; } = DEFAULT_DUAL_MODE;
#if !NETSTANDARD && !UAP
        public IPProtectionLevel IpProtectionLevel { get; set; } = DEFAULT_IP_PROTECTION_LEVEL;
        public bool IsAsync { get; set; } = DEFAULT_IS_ASYNC;
#endif

        /// <inheritdoc />
        public override IReadOnlyCollection<string> Schemes => SupportedSchemes;

        /// <inheritdoc />
        internal override async Task<Listener> ListenAsync(Uri address, WebSocketListenerOptions options)
        {
            if (address == null) throw new ArgumentNullException(nameof(address));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var isSecureWebsocket = string.Equals(address.Scheme, "wss", StringComparison.OrdinalIgnoreCase);
            var isRawTcp = string.Equals(address.Scheme, "tcp", StringComparison.OrdinalIgnoreCase);
            var defaultWebsocketPort = isSecureWebsocket ? DEFAULT_SECURE_HTTP_PORT : DEFAULT_HTTP_PORT;
            var port = address.Port;
            if (port <= 0 && !isRawTcp)
            {
                port = defaultWebsocketPort;
            }
            var endPoints = default(EndPoint[]);
            var ipAddress = default(IPAddress);
            if (IPAddress.TryParse(address.DnsSafeHost, out ipAddress))
            {
                endPoints = new EndPoint[] { new IPEndPoint(ipAddress, port) };
            }
            else
            {
                var ipAddresses = await Dns.GetHostAddressesAsync(address.DnsSafeHost).ConfigureAwait(false);
                endPoints = ipAddresses.ConvertAll(ipAddr => (EndPoint)new IPEndPoint(ipAddr, port));
            }

            return new TcpListener(this, endPoints, options);
        }

        /// <inheritdoc />
        internal override bool ShouldUseSsl(Uri address)
        {
            if (address == null) throw new ArgumentNullException(nameof(address));

            return string.Equals(address.Scheme, "wss", StringComparison.OrdinalIgnoreCase);
        }
        /// <inheritdoc />
        protected override EndPoint GetRemoteEndPoint(Uri address)
        {
            if (address == null) throw new ArgumentNullException(nameof(address));

            var remoteIpAddress = default(IPAddress);
            var isSecure = this.ShouldUseSsl(address);
            var port = address.Port <= 0 ? (isSecure ? DEFAULT_SECURE_HTTP_PORT : DEFAULT_HTTP_PORT) : address.Port;
            if (IPAddress.TryParse(address.DnsSafeHost, out remoteIpAddress))
            {
                return new IPEndPoint(remoteIpAddress, port);
            }

            if (this.DualMode)
                return new DnsEndPoint(address.DnsSafeHost, port, AddressFamily.InterNetworkV6);
            else
                return new DnsEndPoint(address.DnsSafeHost, port, AddressFamily.InterNetwork);

        }
        /// <inheritdoc />
        protected override ProtocolType GetProtocolType(Uri address, EndPoint remoteEndPoint)
        {
            return ProtocolType.Tcp;
        }
        /// <inheritdoc />
        protected override void SetupClientSocket(Socket socket, EndPoint remoteEndPoint)
        {
            if (this.DualMode && remoteEndPoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                socket.DualMode = true;
            }

            if (this.LingerState != null)
                socket.LingerState = this.LingerState;
            socket.NoDelay = this.NoDelay;
            socket.ReceiveBufferSize = this.ReceiveBufferSize;
            socket.ReceiveTimeout = (int)this.ReceiveTimeout.TotalMilliseconds + 1;
            socket.SendBufferSize = this.SendBufferSize;
            socket.SendTimeout = (int)this.SendTimeout.TotalMilliseconds + 1;
#if !NETSTANDARD && !UAP
            if (this.IpProtectionLevel != IPProtectionLevel.Unspecified)
            {
                socket.SetIPProtectionLevel(this.IpProtectionLevel);
            }
            socket.UseOnlyOverlappedIO = this.IsAsync;
#endif
        }
    }
}
