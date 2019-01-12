using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace Ofl.Search.Elasticsearch
{
    public class IndexWriteOperations<T> : Operations<T>, IIndexWriteOperations<T>
        where T : class
    {
        #region Constructor

        internal IndexWriteOperations(IElasticClient elasticClient, Index<T> index) : 
            base(elasticClient, index)
        { }

        #endregion

        #region IIndexWriteOperations<T> implementation.

        public virtual async Task UpsertAsync(IEnumerable<T> source, CancellationToken cancellationToken)
        {
            // Validate parameters.
            if (source == null) throw new ArgumentNullException(nameof(source));

            // The request.
            IBulkRequest request = new BulkDescriptor().
                Index(Index.Name).
                Type<T>().
                IndexMany(source, (d, t) => d.Document(t));

            // Send the request.
            IBulkResponse response = await ElasticClient.BulkAsync(request, cancellationToken).ConfigureAwait(false);

            // Throw if there is an error.
            response.ThrowIfError();
        }

        #endregion
    }
}
