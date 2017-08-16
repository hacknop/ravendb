﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class JsonDeserializationValidator
    {
        [Conditional("DEBUG")]
        public static void Validate()
        {
            var exceptions = new List<Exception>();
            var assembly = typeof(JsonDeserializationValidator).GetTypeInfo().Assembly;
            foreach (var type in assembly.GetTypes())
            {
                var typeInfo = type.GetTypeInfo();
                if (typeInfo.IsAbstract)
                    continue;

                if (typeInfo.IsSubclassOf(typeof(CommandBase)) == false)
                    continue;

                if (JsonDeserializationCluster.Commands.TryGetValue(type.Name, out Func<JsonOperationContext,BlittableJsonReaderObject, CommandBase> _))
                    continue;

                exceptions.Add(new InvalidOperationException($"Missing deserialization routine in '{nameof(JsonDeserializationCluster)}.{nameof(JsonDeserializationCluster.Commands)}' for '{type.Name}'."));
            }

            if (exceptions.Count == 0)
                return;

            throw new AggregateException(exceptions);
        }
    }
}
