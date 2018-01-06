using Nest;
using System;

namespace Ofl.Search.Elasticsearch
{
    internal static class ObjectExtensions
    {
        public static Id ToId(this object id)
        {
            // Validate parameters.
            if (id == null) throw new ArgumentNullException(nameof(id));

            // Switch on type.
            switch (id)
            {
                // Supported types only.
                case Guid guid:
                    return new Id(guid.ToString("D"));
                case Int64 int64:
                    return new Id(int64);
                case Int32 int32:
                    return new Id(int32);
                case string str:
                    return new Id(str);

                default:
                    throw new ArgumentException($"The { nameof(id)} parameter is an unsupported type of { id.GetType() }.");
            }
        }
    }
}
