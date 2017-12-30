using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public abstract class Index : Search.Index
    {
        #region Constructor

        protected Index(IElasticClientFactory elasticClientFactory, string name) : base(name)
        {
            // Validate parameters.
            _elasticClientFactory = elasticClientFactory ?? throw new ArgumentNullException(nameof(elasticClientFactory));
        }

        #endregion

        #region Instance, read-only state.

        private readonly IElasticClientFactory _elasticClientFactory;

        #endregion

        #region Helpers.

        protected Task<IElasticClient> CreateElasticClientAsync(CancellationToken cancellationToken)
        {
            // Just call the implementation.
            return CreateElasticClientImplementationAsync(_elasticClientFactory, cancellationToken);
        }

        protected virtual Task<IElasticClient> CreateElasticClientImplementationAsync(IElasticClientFactory elasticClientFactory,
            CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (elasticClientFactory == null) throw new ArgumentNullException(nameof(elasticClientFactory));

            // Return the default.
            return elasticClientFactory.CreateClientAsync(cancellationToken);
        }

        #endregion

        #region To-be overridden

        protected abstract Task PopulateAsync(CancellationToken cancellationToken);

        #endregion

        #region Helpers.

        private async Task<IDeleteIndexResponse> DestroyImplementationAsync(CancellationToken cancellationToken)
        {
            // Create the client.
            IElasticClient client = await CreateElasticClientAsync(cancellationToken).ConfigureAwait(false);

            // The request.
            IDeleteIndexRequest request = new DeleteIndexDescriptor(Indices.Index(new IndexName { Name = Name }));

            // Delete and return the response.
            return await client.DeleteIndexAsync(request, cancellationToken).ConfigureAwait(false);
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

        public override Task<IIndexWriteOperations<T>> GetWriteOperationsAsync<T>(CancellationToken cancellationToken)
        {
            // Create the write operations.
            IIndexWriteOperations<T> ops = new IndexWriteOperations<T>(CreateElasticClientAsync, this);

            // Return.
            return Task.FromResult(ops);
        }

        #region Overrides of Index

        public override Task<IIndexReadOperations<T>> GetReadOperationsAsync<T>(CancellationToken cancellationToken)
        {
            // Create the read operations.
            IIndexReadOperations<T> ops = new IndexReadOperations<T>(CreateElasticClientAsync, this);

            // Return.
            return Task.FromResult(ops);
        }

        #endregion

        #endregion
    }
}
