using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public static class ElasticClientFactoryExtensions
    {
        public static Task<IElasticClient> CreateClientAsync(this IElasticClientFactory elasticClientFactory,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (elasticClientFactory == null) throw new ArgumentNullException(nameof(elasticClientFactory));

            // Call the client factory.
            return elasticClientFactory.CreateClientAsync(cs => cs, cancellationToken);
        }
    }
}
