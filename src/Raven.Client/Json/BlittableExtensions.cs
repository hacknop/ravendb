using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal static class BlittableExtensions
    {
        public static IEnumerable<Tuple<object, object>> SelectTokenWithRavenSyntaxReturningFlatStructure(
            this BlittableJsonReaderBase self, 
            JsonOperationContext ctx,
            string path,
            bool createSnapshots = false)
        {
            var pathParts = path.Split(new[] { "[]." }, StringSplitOptions.RemoveEmptyEntries);
            var result = new BlittablePath(ctx, pathParts[0]).Evaluate(self, false);

            if (pathParts.Length == 1)
            {
                yield return Tuple.Create(result, (object)self);
                yield break;
            }

            if (result is BlittableJsonReaderObject)
            {
                var blitResult = result as BlittableJsonReaderObject;
                blitResult.TryGetMember(Constants.Json.Fields.Values, out result);

                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (var i = 0; i < blitResult.Count; i++)
                {
                    blitResult.GetPropertyByIndex(ctx, i, ref prop);

                    if (prop.Value is BlittableJsonReaderBase)
                    {
                        var itemAsBlittable = (BlittableJsonReaderBase)prop.Value;
                        foreach (var subItem in itemAsBlittable.SelectTokenWithRavenSyntaxReturningFlatStructure(ctx, string.Join("[].", pathParts.Skip(1).ToArray())))
                        {
                            yield return subItem;
                        }
                    }
                    else
                    {
                        yield return Tuple.Create(prop.Value, result);
                    }
                }
            }
            else if (result is BlittableJsonReaderArray)
            {
                var blitResult = result as BlittableJsonReaderArray;
                for (var i = 0; i < blitResult.Length; i++)
                {
                    var item = blitResult.GetValueTokenTupleByIndex(ctx, i).Value;

                    if (item is BlittableJsonReaderBase)
                    {
                        var itemAsBlittable = item as BlittableJsonReaderBase;
                        foreach (var subItem in itemAsBlittable.SelectTokenWithRavenSyntaxReturningFlatStructure(ctx, string.Join("[].", pathParts.Skip(1).ToArray())))
                        {
                            yield return subItem;
                        }
                    }
                    else
                    {
                        yield return Tuple.Create(item, result);
                    }
                }
            }
            else
            {
                throw new ArgumentException("Illegal path");
            }
        }
    }
}
