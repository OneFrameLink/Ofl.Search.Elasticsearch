using System;
using Nest;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Ofl.Core.Linq.Expressions;
using Ofl.Core.Reflection;
using Ofl.Core.Linq;

namespace Ofl.Search.Elasticsearch
{
    public static class SearchRequestExtensions
    {
        public static SearchDescriptor<T> UpdateSearchDescriptor<T>(this SearchDescriptor<T> selector, 
            Index index, SearchRequest request)
            where T : class
        {
            // Validate parameters.
            if (selector == null) throw new ArgumentNullException(nameof(selector));
            if (index == null) throw new ArgumentNullException(nameof(index));
            request.Validate();

            // Start updating.
            selector = selector.
                Index(Indices.Index(new [] { index.Name })).
                Type<T>();

            // Set take, skip.
            if (request.Skip > 0)
                selector = selector.Skip(request.Skip);
            if (request.Take != null)
                selector = selector.Size(request.Take.Value);

            // Minimum score.
            if (request.MinimumScore != null)
                selector = selector.MinScore((double) request.MinimumScore.Value);

            // Query all of the fields.
            selector = selector.Query(
                q => q.Bool(
                    b => b.
                        Should(request.QueryAllFields<T>()).
                        Filter(request.Filter<T>())
                    )
            );

            // Return the descriptor.
            return selector;
        }

        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> Filter<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Call the implementation.
            return request.FilterImplementation<T>();
        }

        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> FilterImplementation<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            Debug.Assert(request != null);

            // If there is no filters, break.
            if (request.Filters == null) yield break;

            // Cycle through the key/value pairs.
            foreach (KeyValuePair<string, object> pair in request.Filters)
                // Yield the query container.
                yield return d => d.Term(pair.Key, pair.Value);
        }

        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> QueryAllFields<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Call the implementation.
            return request.QueryAllFieldsImplementation<T>();
        }

        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> QueryAllFieldsImplementation<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            Debug.Assert(request != null);

            // Get the expressions.
            IEnumerable<Expression<Func<T, object>>> expressions = GetExpressions<T>();

            // Cycle through the properties, yield the function.
            foreach (Expression<Func<T, object>> expression in expressions)
                // Yield.
                yield return s => s.Match(m => m.Field(expression).Query(request.Query));
        }

        private static readonly ConcurrentDictionary<Type, IReadOnlyCollection<Expression>> ExpressionsByType = 
            new ConcurrentDictionary<Type, IReadOnlyCollection<Expression>>();

        private static IEnumerable<Expression<Func<T, object>>> GetExpressions<T>()
            where T : class
        {
            // The type.
            Type type = typeof(T);

            // Get or Add.
            return ExpressionsByType.GetOrAdd(type,
                t => (
                    from p in t.GetPropertiesWithPublicInstanceGetters()
                    where p.PropertyType == typeof(string) || typeof(IEnumerable<string>).IsAssignableFrom(p.PropertyType)
                    let attr = p.GetCustomAttribute<IndexingAttribute>(true)
                    where attr == null || attr.Indexing != Indexing.None
                    select p.CreateGetPropertyLambdaExpression<T>()
                ).
                Cast<Expression>().
                ToReadOnlyCollection()
            ).Cast<Expression<Func<T, object>>>();
        }
    }
}
