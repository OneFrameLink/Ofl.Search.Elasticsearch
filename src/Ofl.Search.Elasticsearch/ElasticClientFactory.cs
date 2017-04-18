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

        public ElasticClientFactory(IOptions<ElasticsearchConfiguration> configuration)
        {
            // Validate parameters.
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            // Assign values.
            _configuration = configuration.Value;
        }

        #endregion

        #region Instance, read-only state.

        private readonly ElasticsearchConfiguration _configuration;

        #endregion

        #region Implementation of IElasticClientFactory

        public Task<IElasticClient> CreateClientAsync(Func<ConnectionSettings, ConnectionSettings> connectionSettingsModifier, 
            CancellationToken cancellationToken)
        {
            // The connection pool.
            var connectionPool = new SingleNodeConnectionPool(_configuration.Url);

            // The settings.
            var connectionSettings = new ConnectionSettings(connectionPool);

            // Enable compression if set.
            if (_configuration.EnableHttpCompression)
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
