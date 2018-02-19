using System;
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

        public virtual async Task<IReadOnlyCollection<T>> GetAsync(IEnumerable<object> ids, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (ids == null) throw new ArgumentNullException(nameof(ids));

            // Create the client.
            IElasticClient client = await CreateElasticClientAsync(cancellationToken).
                ConfigureAwait(false);

            // Create the request.
            ISearchRequest Request(SearchDescriptor<T> s) => s.Query(d => d.Ids(i => i.Types(typeof(T))
                .Values(ids.Select(id => id.ToId()))));

            // Create the search descriptor
            ISearchResponse<T> response = await client.SearchAsync((Func<SearchDescriptor<T>, ISearchRequest>) Request, cancellationToken)
                .ConfigureAwait(false);

            // Return.
            return response.Documents;
        }

        #endregion
    }
}
