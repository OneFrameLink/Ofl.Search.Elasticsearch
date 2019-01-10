using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public static class IndexExtensions
    {
        public static IndexName CreateIndexName<T>(this IIndex<T> index)
            where T : class
        {
            // Validate parameters.
            if (index == null) throw new ArgumentNullException(nameof(index));

            // Create and return.
            return IndexName.From<T>();
        }
    }
}
