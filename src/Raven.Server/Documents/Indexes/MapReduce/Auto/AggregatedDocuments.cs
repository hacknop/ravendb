using Sparrow.Json;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class AggregatedDocuments : AggregationResult
    {
        private readonly JsonOperationContext _ctx;
        private readonly List<Document> _outputs;

        public AggregatedDocuments(JsonOperationContext ctx, List<Document> results)
        {
            _ctx = ctx;
            _outputs = results;
        }

        public override int Count => _outputs.Count;

        public override IEnumerable<object> GetOutputs()
        {
            return _outputs;
        }

        public override IEnumerable<BlittableJsonReaderObject> GetOutputsToStore()
        {
            foreach (var output in _outputs)
            {
                yield return output.Data;
            }
        }

        public override void Dispose()
        {
            for (int i = _outputs.Count - 1; i >= 0; i--)
            {
                _outputs[i].Data.Dispose(_ctx);
            }
            _outputs.Clear();
        }
    }

}
