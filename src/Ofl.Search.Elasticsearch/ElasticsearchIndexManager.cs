using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public class ElasticsearchIndexManager : IndexManager
    {
        #region Constructor

        public ElasticsearchIndexManager(
            IElasticClientFactory elasticClientFactory,
            IIndexFactory indexFactory
        ) : base(indexFactory)
        {
            // Validate parameters.
            _elasticClient = elasticClientFactory?.CreateClient() ??
                throw new ArgumentNullException(nameof(elasticClientFactory));
        }

        #endregion

        #region Instance, read-only state

        private readonly IElasticClient _elasticClient;

        #endregion

        #region Helpers

        private async Task<IReadOnlyDictionary<string, Type>> MapIndexNamesToTypesAsync(IEnumerable<string> indices,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (indices == null) throw new ArgumentNullException(nameof(indices));

            // Get the tasks.
            IEnumerable<Task<IIndex>> tasks = indices.Select(i => GetIndexAsync(i, cancellationToken));

            // Wait.
            IIndex[] indexes = await Task.WhenAll(tasks).ConfigureAwait(false);

            // Map.
            return indexes.ToReadOnlyDictionary(i => i.Name, i => i.Type);
        }

        #endregion

        #region Overrides

        public override async Task<MultiIndexGetResponse> MultiIndexGetAsync(MultiIndexGetRequest request, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Get all of the index types.
            IReadOnlyDictionary<string, Type> indexes =
                await MapIndexNamesToTypesAsync(request.Ids.Select(g => g.Key), cancellationToken)
                    .ConfigureAwait(false);

            // Create the request.
            IMultiGetResponse response = await _elasticClient
                .MultiGetAsync(descriptor => request.Ids.Aggregate(descriptor, (d, g) => {
                    // The type.
                    Type type = OpenMultiIndexGetDescriptorReducer.MakeGenericType(indexes[g.Key]);

                    // Create the type.
                    var reducer = (IMultiIndexGetDescriptorReducer) Activator.CreateInstance(type);

                    // Reduce.
                    return reducer.Reduce(d, g);
                }), cancellationToken)
                .ConfigureAwait(false);

            // Map the results and return.
            return new MultiIndexGetResponse {
                Request = request,
                TotalHits = response.Hits.Count,
                Hits = response
                    .Hits
                    .Select(h => h.ToHit<object, object>())
                    .ToReadOnlyCollection()
            };
        }

        #endregion

        #region Helper class

        private static readonly Type OpenMultiIndexGetDescriptorReducer = typeof(MultiIndexGetDescriptorReducer<>);

        private interface IMultiIndexGetDescriptorReducer
        {
            MultiGetDescriptor Reduce(
                MultiGetDescriptor descriptor,
                IGrouping<string, object> group);
        }

        private class MultiIndexGetDescriptorReducer<T> : IMultiIndexGetDescriptorReducer
            where T : class
        {
            public MultiGetDescriptor Reduce(MultiGetDescriptor descriptor,
                IGrouping<string, object> group)
            {
                // Validate parameters.
                if (descriptor == null) throw new ArgumentNullException(nameof(descriptor));
                if (group == null) throw new ArgumentNullException(nameof(group));

                // Get the IDs.
                IEnumerable<Id> ids = group.Select(id => id.ToId());

                // Aggregate and return.
                return descriptor.GetMany<T>(ids, (d, id) => d.Index(group.Key));
            }
        }

        #endregion
    }
}
