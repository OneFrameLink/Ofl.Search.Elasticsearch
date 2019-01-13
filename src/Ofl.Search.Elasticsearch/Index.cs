using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public abstract class Index<T> : Search.Index<T>
        where T : class
    {
        #region Constructor

        protected Index(string name, IElasticClient elasticClient) : base(name)
        {
            // Validate parameters.
            ElasticClient = elasticClient
                ?? throw new ArgumentNullException(nameof(elasticClient));
        }

        #endregion

        #region Instance, read-only state.

        protected readonly IElasticClient ElasticClient;

        #endregion

        #region Overrides of Index

        public override async Task<bool> ExistsAsync(CancellationToken cancellationToken)
        {
            // Create the request.
            IIndexExistsRequest request = new IndexExistsDescriptor(Indices.Index<T>());

            // Get the response.
            IExistsResponse response = await ElasticClient.IndexExistsAsync(request, cancellationToken)
                .ConfigureAwait(false);

            // Throw if invalid.
            response.ThrowIfError();

            // Return the response.
            return response.Exists;
        }

        public override async Task DestroyAsync(CancellationToken cancellationToken)
        {
            // The request.
            IDeleteIndexRequest request = new DeleteIndexDescriptor(Indices.Index<T>());

            // Delete and return the response.
            IDeleteIndexResponse response = await ElasticClient
                .DeleteIndexAsync(request, cancellationToken).ConfigureAwait(false);

            // If it failed, throw an exception.
            response.ThrowIfError();
        }

        public override Task<IIndexWriteOperations<T>> GetWriteOperationsAsync(CancellationToken cancellationToken)
        {
            // Create the write operations.
            IIndexWriteOperations<T> ops = new IndexWriteOperations<T>(ElasticClient, this);

            // Return.
            return Task.FromResult(ops);
        }

        public override Task<IIndexReadOperations<T>> GetReadOperationsAsync(CancellationToken cancellationToken)
        {
            // Create the read operations.
            IIndexReadOperations<T> ops = new IndexReadOperations<T>(ElasticClient, this);

            // Return.
            return Task.FromResult(ops);
        }

        #endregion
    }
}
