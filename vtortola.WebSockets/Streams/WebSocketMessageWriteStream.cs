using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace vtortola.WebSockets
{
    public abstract class WebSocketMessageWriteStream : WebSocketMessageStream
    {
        public sealed override bool CanWrite => true;

        [NotNull]
        public WebSocketExtensionFlags ExtensionFlags { get; }

        protected WebSocketMessageWriteStream()
        {
            this.ExtensionFlags = new WebSocketExtensionFlags();
        }

        /// <inheritdoc />
        [Obsolete("Reading from the write stream is not allowed", true)]
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }
    }
}
