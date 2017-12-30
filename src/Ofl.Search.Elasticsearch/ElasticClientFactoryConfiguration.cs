using System;

namespace Ofl.Search.Elasticsearch
{
    public class ElasticClientFactoryConfiguration
    {
        public Uri Url { get; set; }

        public bool EnableHttpCompression { get; set; }
    }
}
