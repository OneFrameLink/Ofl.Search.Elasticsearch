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

            // Create the request.
            ISearchRequest Request(SearchDescriptor<T> searchDescriptor) {
                // Set the query.
                searchDescriptor = searchDescriptor
                    .Query(
                        d => d
                            .Ids(i => i
                                .Types(typeof(T))
                                .Values(request.Ids.Select(id => id.ToId()))))
                    .Index(Index.Name);

                // If skip is set, then set it.
                if (request.Skip > 0)
                    // Set.
                    searchDescriptor = searchDescriptor.Skip(request.Skip);

                // If only taking a certain amount, do so here.
                if (request.Take != null)
                    // Set.
                    searchDescriptor = searchDescriptor.Take(request.Take.Value);

                // Return the search descriptor.
                return searchDescriptor;
            }

            // Create the search descriptor
            ISearchResponse<T> response = await client.SearchAsync((Func<SearchDescriptor<T>, ISearchRequest>)Request, cancellationToken)
                .ConfigureAwait(false);

            // Create the response and return.
            return new GetResponse<T> {
                Request = request,
                TotalHits = response.Total,
                Hits = response.Hits.Select(h => h.ToHit(null)).ToReadOnlyCollection()
            };
        }

        #endregion
    }
}
