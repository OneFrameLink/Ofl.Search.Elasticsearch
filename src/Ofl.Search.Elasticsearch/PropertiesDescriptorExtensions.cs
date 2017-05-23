﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Nest;
using Ofl.Linq;
using Ofl.Reflection;

namespace Ofl.Search.Elasticsearch
{
    public static class PropertiesDescriptorExtensions
    {
        public static PropertiesDescriptor<T> Int32<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            Func<NumberPropertyDescriptor<T>, INumberProperty> selector)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // Modify and return.
            return propertiesDescriptor.NumberProperty(selector, NumberType.Integer);
        }

        public static PropertiesDescriptor<T> Int64<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            Func<NumberPropertyDescriptor<T>, INumberProperty> selector)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // Modify and return.
            return propertiesDescriptor.NumberProperty(selector, NumberType.Long);
        }

        private static PropertiesDescriptor<T> NumberProperty<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            Func<NumberPropertyDescriptor<T>, INumberProperty> selector, NumberType numberType)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // Modify and return.
            return propertiesDescriptor.Number(np => selector(np.Type(numberType)));
        }

        public static PropertiesDescriptor<T> Strings<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            params Expression<Func<T, object>>[] excludedProperties)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (excludedProperties == null) throw new ArgumentNullException(nameof(excludedProperties));

            // Call the implementation.
            return propertiesDescriptor.StringsImplementation(null, excludedProperties);
        }

        public static PropertiesDescriptor<T> Strings<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            string analyzer, params Expression<Func<T, object>>[] excludedProperties)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (excludedProperties == null) throw new ArgumentNullException(nameof(excludedProperties));
            if (string.IsNullOrWhiteSpace(analyzer)) throw new ArgumentNullException(nameof(analyzer));

            // Call the implementation.
            return propertiesDescriptor.StringsImplementation(analyzer, excludedProperties);
        }

        private static PropertiesDescriptor<T> StringsImplementation<T>(this PropertiesDescriptor<T> propertiesDescriptor,
            string analyzer, params Expression<Func<T, object>>[] excludedProperties)
            where T : class
        {
            // Validate parameters.
            if (propertiesDescriptor == null) throw new ArgumentNullException(nameof(propertiesDescriptor));
            if (excludedProperties == null) throw new ArgumentNullException(nameof(excludedProperties));

            // Get the expressions, place in a set.
            ISet<PropertyInfo> excludedPropertyInfos = excludedProperties.Select(e => e.GetPropertyInfo()).ToHashSet();

            // Cycle through all public instance properties not in the exclude list and
            // also returns a string.
            foreach (PropertyInfo propertyInfo in typeof(T).GetPropertiesWithPublicInstanceGetters().
                Where(p => p.PropertyType == typeof(string) && !excludedPropertyInfos.Contains(p)))
            {
                // Update the properties descriptor.
                propertiesDescriptor = propertiesDescriptor.String(d => {
                    // Set the name.  Use the property info so name conversion
                    // takes place correctly.
                    d = d.Name(propertyInfo);

                    // If analyzer is not null.
                    if (analyzer != null) d = d.Analyzer(analyzer);

                    // Return the descriptor.
                    return d;
                });
            }

            // Return the properties descriptor.
            return propertiesDescriptor;
        }
    }
}
