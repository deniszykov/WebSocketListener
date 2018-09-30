using JetBrains.Annotations;
using vtortola.WebSockets.Transports;

namespace vtortola.WebSockets
{
    [PublicAPI]
    public interface IHttpFallback
    {
        void Post([NotNull] IHttpRequest request, [NotNull] NetworkConnection networkConnection);
    }
}
