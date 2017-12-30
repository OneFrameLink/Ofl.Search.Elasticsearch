using System;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.Extensions.Options;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public class ElasticClientFactory : IElasticClientFactory
    {
        #region Constructor

        public ElasticClientFactory(IOptions<ElasticClientFactoryConfiguration> elasticClientFactoryConfigurationOptions)
        {
            // Validate parameters.
            _elasticClientFactoryConfigurationOptions = elasticClientFactoryConfigurationOptions ??
                throw new ArgumentNullException(nameof(elasticClientFactoryConfigurationOptions));
        }

        #endregion

        #region Instance, read-only state.

        private readonly IOptions<ElasticClientFactoryConfiguration> _elasticClientFactoryConfigurationOptions;

        #endregion

        #region Implementation of IElasticClientFactory

        public Task<IElasticClient> CreateClientAsync(Func<ConnectionSettings, ConnectionSettings> connectionSettingsModifier, 
            CancellationToken cancellationToken)
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

            // Call the modifier.
            connectionSettings = connectionSettingsModifier(connectionSettings);
            
            // Create the client.
            IElasticClient client = new ElasticClient(connectionSettings);

            // Return the client.
            return Task.FromResult(client);
        }

        #endregion
    }
}
