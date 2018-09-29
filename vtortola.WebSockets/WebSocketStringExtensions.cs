using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace vtortola.WebSockets
{
    public static class WebSocketStringExtensions
    {
        private static readonly UTF8Encoding Utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);

        [NotNull, ItemCanBeNull]
        public static async Task<string> ReadStringAsync([NotNull] this WebSocket webSocket, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (webSocket == null) throw new ArgumentNullException(nameof(webSocket));

            using (var readStream = await webSocket.ReadMessageAsync(cancellationToken).ConfigureAwait(false))
            {
                if (readStream == null)
                    return null;

                using (var reader = new StreamReader(readStream, Utf8NoBom))
                    return await reader.ReadToEndAsync().ConfigureAwait(false);
            }
        }

        public static async Task WriteStringAsync([NotNull] this WebSocket webSocket, [NotNull] string data, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (webSocket == null) throw new ArgumentNullException(nameof(webSocket));
            if (data == null) throw new ArgumentNullException(nameof(data));

            cancellationToken.ThrowIfCancellationRequested();

            using (var msg = webSocket.CreateMessageWriter(WebSocketMessageType.Text))
            using (var writer = new StreamWriter(msg, Utf8NoBom))
            {
                await writer.WriteAsync(data).ConfigureAwait(false);
                await writer.FlushAsync().ConfigureAwait(false);
                await msg.CloseAsync().ConfigureAwait(false);
            }
        }
    }
}
