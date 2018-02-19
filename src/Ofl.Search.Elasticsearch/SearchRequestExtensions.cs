using System;
using Nest;
using System.Collections.Generic;

namespace Ofl.Search.Elasticsearch
{
    public static class SearchRequestExtensions
    {
        public static SearchDescriptor<T> UpdateSearchDescriptor<T>(this SearchDescriptor<T> selector, 
            Index index, SearchRequest request, string preAndPostTag)
            where T : class
        {
            // Validate parameters.
            if (selector == null) throw new ArgumentNullException(nameof(selector));
            if (index == null) throw new ArgumentNullException(nameof(index));
            if (string.IsNullOrWhiteSpace(preAndPostTag)) throw new ArgumentNullException(nameof(preAndPostTag));
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
                        Should(request.QueryStringQuery<T>()).
                        Filter(request.Filter<T>())
                    )
            );

            // Highlight, if desired.
            if (request.Highlight)
                // Highlight.
                selector = selector.Highlight(
                    h => h
                        .PreTags(preAndPostTag)
                        .PostTags(preAndPostTag)
                        .NumberOfFragments(0)
                        .RequireFieldMatch(false)
                        .Fields(f => f.AllField())
                );

            // Return the descriptor.
            return selector;
        }

        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> Filter<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // The implementation.
            IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> Implementation() {
                // If there is no filters, break.
                if (request.Filters == null) yield break;

                // Cycle through the key/value pairs.
                foreach (Filter filter in request.Filters)
                {
                    // Switch on the operation.
                    if (string.IsNullOrWhiteSpace(filter.Operation))
                        // Equals.
                        yield return d => d.Term(filter.Field, filter.Value);

                    // If the type is a date.
                    if (string.Compare(filter.Operation, "lte", StringComparison.OrdinalIgnoreCase) == 0)
                        // Less than or equal.
                        yield return d => d.DateRange(r => r.GreaterThan(DateMath.FromString("")));
                }
            }

            // Call the implementation.
            return Implementation();
        }


        private static IEnumerable<Func<QueryContainerDescriptor<T>, QueryContainer>> QueryStringQuery<T>(this SearchRequest request)
            where T : class
        {
            // Validate parameters.
            if (request == null) throw new ArgumentNullException(nameof(request));

            // Just a query string query, as per:
            // https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-query-string-query.html
            return new Func<QueryContainerDescriptor<T>, QueryContainer>[] {
                s => s.QueryString(d => d.Query(request.Query))
            };
        }
    }
}
