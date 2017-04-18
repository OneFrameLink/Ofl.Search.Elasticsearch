using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public static class ResponseExtensions
    {
        public static void ThrowIfError(this IResponse response)
        {
            // Validate parameters.
            if (response == null) throw new ArgumentNullException(nameof(response));

            // If not valid, throw.
            if (!response.IsValid)
                throw response.OriginalException ?? new InvalidOperationException(response.ServerError.Error.Reason);
        }
    }
}
