using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public static class HitExtensions
    {
        public static Hit<T> ToHit<T>(this IHit<T> hit)
            where T : class
        {
            // Validate parameters.
            if (hit == null) throw new ArgumentNullException(nameof(hit));

            // Map.
            return new Hit<T> {
                Item = hit.Source,
                Score = (decimal?) hit.Score,
            };
        }
    }
}
