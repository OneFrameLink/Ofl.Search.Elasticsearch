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
            _elasticClient = elasticClient
                ?? throw new ArgumentNullException(nameof(elasticClient));

            // Assign values.
            _indexName = IndexName.From<T>();
        }

        #endregion

        #region Instance, read-only state.

        private readonly IElasticClient _elasticClient;

        private readonly IndexName _indexName;

        #endregion

        #region To-be overridden

        protected abstract Task PopulateAsync(CancellationToken cancellationToken);

        #endregion

        #region Helpers.

        private async Task<IDeleteIndexResponse> DestroyImplementationAsync(CancellationToken cancellationToken)
        {
            // The request.
            IDeleteIndexRequest request = new DeleteIndexDescriptor(Indices.Index(_indexName));

            // Delete and return the response.
            return await _elasticClient.DeleteIndexAsync(request, cancellationToken).ConfigureAwait(false);
        }

        #endregion

        #region Overrides of Index

        public override async Task RegenerateAsync(CancellationToken cancellationToken)
        {
            // Destroy.
            await DestroyImplementationAsync(cancellationToken).ConfigureAwait(false);

            // Create.
            await CreateAsync(cancellationToken).ConfigureAwait(false);

            // Populate.
            await PopulateAsync(cancellationToken);
        }

        public override async Task DestroyAsync(CancellationToken cancellationToken)
        {
            // Call the implementation.
            IDeleteIndexResponse response = await DestroyImplementationAsync(cancellationToken).
                ConfigureAwait(false);

            // If it failed, throw an exception.
            response.ThrowIfError();
        }

        public override Task<IIndexWriteOperations<T>> GetWriteOperationsAsync(CancellationToken cancellationToken)
        {
            // Create the write operations.
            IIndexWriteOperations<T> ops = new IndexWriteOperations<T>(_elasticClient, this);

            // Return.
            return Task.FromResult(ops);
        }

        #region Overrides of Index

        public override Task<IIndexReadOperations<T>> GetReadOperationsAsync(CancellationToken cancellationToken)
        {
            // Create the read operations.
            IIndexReadOperations<T> ops = new IndexReadOperations<T>(_elasticClient, this);

            // Return.
            return Task.FromResult(ops);
        }

        #endregion

        #endregion
    }
}
