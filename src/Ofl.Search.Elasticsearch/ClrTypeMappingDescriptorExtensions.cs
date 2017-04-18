using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Nest;
using Ofl.Core.Reflection;
using System.ComponentModel.DataAnnotations;
using System.Linq.Expressions;
using Ofl.Core.Linq;
using Ofl.Core.Linq.Expressions;

namespace Ofl.Search.Elasticsearch
{
    public static class ClrTypeMappingDescriptorExtensions
    {
        public static ClrTypeMappingDescriptor<T> KeyIsIdProperty<T>(this ClrTypeMappingDescriptor<T> selector)
            where T : class
        {
            // Validate parameters.
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            // Get the key properties on T.
            IReadOnlyCollection<PropertyInfo> propertyInfos = typeof(T).
                GetPropertiesWithPublicInstanceGetters().
                Where(p => p.GetCustomAttribute<KeyAttribute>(true) != null).
                ToReadOnlyCollection();

            // If no properties, then throw.
            if (propertyInfos.Count == 0) throw new InvalidOperationException($"The type parameter { nameof(T) } does not have a property decorated with a { nameof(KeyAttribute)}.");
            
            // If greater than 1, throw.
            if (propertyInfos.Count > 1) throw new InvalidOperationException($"The type parameter { nameof(T) } has more than one property decorated with a { nameof(KeyAttribute)}.");

            // Get the single item.
            PropertyInfo propertyInfo = propertyInfos.Single();

            // Create the lambda.
            Expression<Func<T, object>> idPropertyExpression = propertyInfo.CreateGetPropertyLambdaExpression<T>();

            // Set that property.
            return selector.IdProperty(idPropertyExpression);
        }
    }
}
