using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Ofl.Search.Elasticsearch
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddElasticsearch(this IServiceCollection serviceCollection,
            IConfiguration elasticClientFactoryConfiguration)
        {
            // Validate parameters.
            if (serviceCollection == null) throw new ArgumentNullException(nameof(serviceCollection));
            if (elasticClientFactoryConfiguration == null) throw new ArgumentNullException(nameof(elasticClientFactoryConfiguration));

            // For ease-of-use.
            var sc = serviceCollection;

            // Add the index factory.
            sc = sc.AddSingleton<IIndexFactory, ElasticIndexFactory>();

            // Configure the elastic client factory.
            sc = sc.AddSingleton<IElasticClientFactory, ElasticClientFactory>();

            // Configuration for said factory.
            sc = sc.Configure<ElasticClientFactoryConfiguration>(elasticClientFactoryConfiguration.Bind);

            // Return the service collection.
            return sc;
        }
    }
}
