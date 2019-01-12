using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.Extensions.Options;
using Nest;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public class ElasticClientFactory : IElasticClientFactory
    {
        #region Constructor

        public ElasticClientFactory(
            IEnumerable<IElasticIndexSource> elasticIndexSources,
            IOptions<ElasticClientFactoryConfiguration> elasticClientFactoryConfigurationOptions
        )
        {
            // Validate parameters.
            _elasticIndexSources = elasticIndexSources?.ToReadOnlyCollection()
                ?? throw new ArgumentNullException(nameof(elasticIndexSources));
            _elasticClientFactoryConfigurationOptions = elasticClientFactoryConfigurationOptions
                ?? throw new ArgumentNullException(nameof(elasticClientFactoryConfigurationOptions));
        }

        #endregion

        #region Instance, read-only state.

        private readonly IOptions<ElasticClientFactoryConfiguration> _elasticClientFactoryConfigurationOptions;

        private readonly IReadOnlyCollection<IElasticIndexSource> _elasticIndexSources;

        #endregion

        #region Implementation of IElasticClientFactory

        public IElasticClient CreateClient()
        {
            // The options.
            ElasticClientFactoryConfiguration elasticClientFactoryConfiguration =
                _elasticClientFactoryConfigurationOptions.Value;

            // The connection pool.
            var connectionPool = new SingleNodeConnectionPool(elasticClientFactoryConfiguration.Url);

            // The settings.
            var connectionSettings = new ConnectionSettings(connectionPool);

            // Enable compression if set.
            if (elasticClientFactoryConfiguration.EnableHttpCompression)
                connectionSettings = connectionSettings.EnableHttpCompression();

            // Set the mapping for the types.
            connectionSettings = _elasticIndexSources.Aggregate(connectionSettings,
                (cs, s) => cs.DefaultMappingFor(s.Type, s.DefineMapping));
            
            // Create the client.
            IElasticClient client = new ElasticClient(connectionSettings);

            // Return the client.
            return client;
        }

        #endregion
    }
}
