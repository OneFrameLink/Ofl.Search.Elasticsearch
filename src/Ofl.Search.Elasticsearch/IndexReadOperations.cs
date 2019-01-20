using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using System.Linq;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public class IndexReadOperations<T> : Operations<T>, IIndexReadOperations<T>
        where T : class
    {
        #region Constructor

        internal IndexReadOperations(IElasticClient elasticClient, Index<T> index) :
            base(elasticClient, index)
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

            // Create the pre and post tag for highlighting.
            string preAndPostTag = CreatePreAndPostTag();

            // Search.
            ISearchResponse<T> response = await ElasticClient.SearchAsync<T>(
                d => d.UpdateSearchDescriptor(Index, request, preAndPostTag), cancellationToken).ConfigureAwait(false);

            // Validate the response.
            response.ThrowIfError();

            // Create a copy of pre and post tag.
            var preAndPostTagCopy = preAndPostTag;

            // Return the response.
            return new SearchResponse<T> {
                Request = request,
                MaximumScore = (decimal) response.MaxScore,
                TotalHits = (int) response.Total,
                Hits = response.Hits.Select(h => h.ToHit(preAndPostTagCopy)).ToReadOnlyCollection()
            };            
        }

        private static string CreatePreAndPostTag() => Guid.NewGuid().ToString("B");

        public virtual async Task<GetResponse<T>> GetAsync(GetRequest request, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Get the Ids.
            IReadOnlyCollection<string> ids = request.Ids
                .Select(id => id.ToString())
                .Distinct()
                .ToReadOnlyCollection();

            // Get the response.
            IMultiGetResponse response = await ElasticClient.MultiGetAsync(
                d => d
                    .Index(Index.Name)
                    .Type<T>()
                    .GetMany<T>(ids),
                cancellationToken).ConfigureAwait(false);

            // Get the hits.
            IReadOnlyCollection<Hit<T>> hits = response
                .Hits
                .Select(d => d.ToHit<object, T>())
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
