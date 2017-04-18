using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public class Operations : Operations<Index>
    {
        #region Constructor

        public Operations(Func<CancellationToken, Task<IElasticClient>> elasticClientFactory, Index index) : base(index)
        {
            // Validate parameters.
            if (elasticClientFactory == null) throw new ArgumentNullException(nameof(elasticClientFactory));

            // Assign values.
            _elasticClientFactory = elasticClientFactory;
        }

        #endregion

        #region Instance, read-only state.

        private readonly Func<CancellationToken, Task<IElasticClient>> _elasticClientFactory;

        #endregion

        #region Helpers.

        protected Task<IElasticClient> CreateElasticClientAsync(CancellationToken cancellationToken)
        {
            // Call the implementation.
            return _elasticClientFactory(cancellationToken);
        }

        #endregion
    }
}
