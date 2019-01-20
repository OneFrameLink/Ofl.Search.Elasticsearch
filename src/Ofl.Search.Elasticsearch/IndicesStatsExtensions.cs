using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    internal static class IndicesStatsExtensions
    {
        public static ElasticsearchIndexStats ToElasticsearchIndexStats(this IndicesStats stats)
        {
            // Validate parameters.
            if (stats == null) throw new ArgumentNullException(nameof(stats));

            // Create the new instance and return.
            return new ElasticsearchIndexStats(
                stats.Total.Documents.Count,
                (long) stats.Total.Store.SizeInBytes
            );
        }
    }
}
