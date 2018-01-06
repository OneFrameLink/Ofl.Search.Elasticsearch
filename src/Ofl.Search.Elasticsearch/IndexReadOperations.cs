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

            // Search.
            ISearchResponse<T> response = await client.SearchAsync<T>(
                d => d.UpdateSearchDescriptor(Index, request), cancellationToken).ConfigureAwait(false);

            // Validate the response.
            response.ThrowIfError();

            // Return the response.
            return new SearchResponse<T> {
                Request = request,
                MaximumScore = (decimal) response.MaxScore,
                TotalHits = (int) response.Total,
                Hits = response.Hits.Select(h => h.ToHit()).ToReadOnlyCollection()
            };            
        }

        public virtual async Task<T> GetAsync(object id, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (id == null) throw new ArgumentNullException(nameof(id));

            // Get the ID.
            Id convertedId = id.ToId();

            // Create the document path.
            var documentPath = new DocumentPath<T>(convertedId);

            // Set the index.
            documentPath.Index(this.Index.Name);

            // Create the client.
            IElasticClient client = await CreateElasticClientAsync(cancellationToken).
                ConfigureAwait(false);

            // Get.
            IGetResponse<T> response = await client.GetAsync(documentPath,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // If not found, throw.
            return response.Found ? response.Source : null;
        }

        #endregion
    }
}
