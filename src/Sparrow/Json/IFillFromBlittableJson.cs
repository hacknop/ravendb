namespace Sparrow.Json
{
    public interface IFillFromBlittableJson
    {
        void FillFromBlittableJson(JsonOperationContext ctx, BlittableJsonReaderObject json);
    }
}
