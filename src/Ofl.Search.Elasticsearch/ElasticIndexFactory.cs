using System;
using System.Collections.Generic;
using System.Linq;
using Nest;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public class ElasticIndexFactory : IIndexFactory
    {
        #region Constructor

        public ElasticIndexFactory(IElasticClientFactory elasticClientFactory,
            IEnumerable<IElasticIndexSource> elasticIndexSources)
        {
            // Validate parameters
            _elasticElasticClientFactory = elasticClientFactory
                ?? throw new ArgumentNullException(nameof(elasticClientFactory));
            _elasticIndexSources = elasticIndexSources?.ToReadOnlyCollection()
                ?? throw new ArgumentNullException(nameof(elasticIndexSources));
        }

        #endregion

        #region Instance, read-only state

        private readonly IElasticClientFactory _elasticElasticClientFactory;

        private readonly IReadOnlyCollection<IElasticIndexSource> _elasticIndexSources;

        #endregion

        #region IIndexFactory implementation

        public IReadOnlyCollection<IIndex> CreateIndices()
        {
            // Create the client.
            IElasticClient client = _elasticElasticClientFactory.CreateClient();

            // Cycle through the sources, create the indexes.
            return _elasticIndexSources
                .Select(s => s.CreateIndex(client))
                .ToReadOnlyCollection();
        }

        #endregion
    }
}
