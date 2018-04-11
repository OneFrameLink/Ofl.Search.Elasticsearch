﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using System.Linq;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public class IndexReadOperations<T> : Operations, IIndexReadOperations<T>
        where T : class
    {
        #region Constructor

        internal IndexReadOperations(Func<CancellationToken, Task<IElasticClient>> elasticClientFactory, Index index) : 
            base(elasticClientFactory, index)
        { }

        #endregion

        #region IIndexReadOperations<T> implementation.

        public virtual async Task<SearchResponse<T>> SearchAsync(SearchRequest request,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Validate search request.
            request.Validate();

            // Create the client.
            IElasticClient client = await CreateElasticClientAsync(cancellationToken).
                ConfigureAwait(false);

            // Create the pre and post tag for highlighting.
            string preAndPostTag = CreatePreAndPostTag();

            // Search.
            ISearchResponse<T> response = await client.SearchAsync<T>(
                d => d.UpdateSearchDescriptor(Index, request, preAndPostTag), cancellationToken).ConfigureAwait(false);

            // Validate the response.
            response.ThrowIfError();

            // Return the response.
            return new SearchResponse<T> {
                Request = request,
                MaximumScore = (decimal) response.MaxScore,
                TotalHits = (int) response.Total,
                Hits = response.Hits.Select(h => h.ToHit(preAndPostTag)).ToReadOnlyCollection()
            };            
        }

        private static string CreatePreAndPostTag() => Guid.NewGuid().ToString("B");

        public virtual async Task<GetResponse<T>> GetAsync(GetRequest request, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Create the client.
            IElasticClient client = await CreateElasticClientAsync(cancellationToken).
                ConfigureAwait(false);

            // Get the Ids.
            IReadOnlyCollection<Id> ids = request.Ids
                .Select(id => id.ToId())
                .Distinct()
                .ToReadOnlyCollection();

            // Get the response.
            IMultiGetResponse response = await client.MultiGetAsync(
                d => d
                    .Index(Index.Name)
                    .Type<T>()
                    .GetMany<Id>(ids), 
                cancellationToken).ConfigureAwait(false);

            // Get the hits.
            IReadOnlyCollection<Hit<T>> hits = response
                .GetMany<T>(ids.Select(id => id.ToString()))
                .Select(h => new Hit<T> { Item = h.Source })
                .ToReadOnlyCollection();

            // Create the response and return.
            return new GetResponse<T> {
                Request = request,
                TotalHits = hits.Count,
                Hits = hits
            };
        }

        #endregion
    }
}
