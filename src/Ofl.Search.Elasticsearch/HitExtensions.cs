using System;
using System.Collections.Generic;
using System.Linq;
using Nest;
using Ofl.Linq;

namespace Ofl.Search.Elasticsearch
{
    public static class HitExtensions
    {
        public static Hit<T> ToHit<T>(this IHit<T> hit, string preAndPostTag)
            where T : class
        {
            // Validate parameters.
            if (hit == null) throw new ArgumentNullException(nameof(hit));

            // Map and return.
            return new Hit<T> {
                Item = hit.Source,
                Score = (decimal?) hit.Score,
                Highlights = hit.Highlights?.ToReadOnlyDictionary(h => h.Key, h => h.Value.ToHighlightOffsets(preAndPostTag))
            };
        }

        private static IReadOnlyCollection<HighlightOffset> ToHighlightOffsets(
            this HighlightHit highlightHit, string preAndPostTag)
        {
            // Validate parameters.
            if (string.IsNullOrWhiteSpace(preAndPostTag)) throw new ArgumentNullException(nameof(preAndPostTag));

            // If null, return null.
            if (highlightHit == null) return null;

            // Get the single string, as fragments should have been reduced to 0.
            string highlight = highlightHit.Highlights.Single();

            // Enumerates offsets.
            IEnumerable<HighlightOffset> EnumerateHighlightHits() {
                // Need to parse.  Split.
                var parts = highlight.Split(new [] { preAndPostTag }, StringSplitOptions.None);

                // The current offset.
                int offset = 0;

                // In a highlight?
                bool inHighlight = false;

                // Cycle through the parts.
                foreach (string part in parts)
                {
                    // If in a highlight yield hit.
                    if (inHighlight)
                    {
                        // Yield.
                        yield return new HighlightOffset {
                            Offset = offset,
                            Length = part.Length
                        };
                    }

                    // Increment the offset by the length.
                    offset += part.Length;

                    // Update whether or not in the highlight.
                    inHighlight = !inHighlight;
                }
            }

            // Materialize the set.
            return EnumerateHighlightHits().ToReadOnlyCollection();
        }
    }
}
