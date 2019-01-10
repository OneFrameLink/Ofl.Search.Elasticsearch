using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public abstract class Operations<T> : Ofl.Search.Operations<Index<T>>
        where T : class
    {
        #region Constructor

        protected Operations(Func<CancellationToken, Task<IElasticClient>> elasticClientFactory, Index<T> index) : base(index)
        {
            // Validate parameters.
            _elasticClientFactory = elasticClientFactory ?? throw new ArgumentNullException(nameof(elasticClientFactory));
        }

        #endregion

        #region Instance, read-only state.

        private readonly Func<CancellationToken, Task<IElasticClient>> _elasticClientFactory;

        #endregion

        #region Helpers.

        protected Task<IElasticClient> CreateElasticClientAsync(CancellationToken cancellationToken) =>
            _elasticClientFactory(cancellationToken);

        #endregion
    }
}
