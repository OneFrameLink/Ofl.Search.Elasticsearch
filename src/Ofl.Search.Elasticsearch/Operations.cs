using System;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public abstract class Operations<T> : Ofl.Search.Operations<Index<T>>
        where T : class
    {
        #region Constructor

        protected Operations(IElasticClient elasticClient, Index<T> index) : base(index)
        {
            // Validate parameters.
            ElasticClient = elasticClient
                ?? throw new ArgumentNullException(nameof(elasticClient));
        }

        #endregion

        #region Instance, read-only state.

        protected IElasticClient ElasticClient { get; }

        #endregion
    }
}
