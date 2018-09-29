using System;
using System.Collections.Generic;
using System.Net;
using JetBrains.Annotations;
using vtortola.WebSockets.Http;

namespace vtortola.WebSockets
{
    [PublicAPI]
    public interface IHttpRequest
    {
        EndPoint LocalEndPoint { get; }
        EndPoint RemoteEndPoint { get; }
        Uri RequestUri { get; }
        Version HttpVersion { get; }
        bool IsSecure { get; }
        CookieCollection Cookies { get; }
        Headers<RequestHeader> Headers { get; }
        IDictionary<string, object> Items { get; }
    }

}
