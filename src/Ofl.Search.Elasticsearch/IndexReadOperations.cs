using System;
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

        internal IndexReadOperations(Func<CancellationToken, Task<IElasticClient>> elasticClientFactory, Index index) : base(elasticClientFactory, index)
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

            // Search.
            ISearchResponse<T> response = await client.SearchAsync<T>(
                d => d.
                    UpdateSearchDescriptor(Index, request).
                    RequestConfiguration(c => c.CancellationToken(cancellationToken))
            ).ConfigureAwait(false);

            // Validate the response.
            response.ThrowIfError();

            // Return the response.
            return new SearchResponse<T> {
                Request = request,
                Results = response.Hits.Select(h => h.Source).ToReadOnlyCollection()
            };            
        }

        #endregion
    }
}
