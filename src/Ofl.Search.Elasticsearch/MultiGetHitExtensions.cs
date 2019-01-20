using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public static class MultiGetHitExtensions
    {
        public static Hit<TTo> ToHit<TFrom, TTo>(this IMultiGetHit<TFrom> hit)
            where TFrom : class
            where TTo : class
        {
            // Validate parameters.
            if (hit == null) throw new ArgumentNullException(nameof(hit));

            // Map and return.
            return new Hit<TTo> {
                Item = hit.Source as TTo,
                Id = hit.Id,
                Index = hit.Index
            };
        }
    }
}
