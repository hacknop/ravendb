using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetDatabaseTopologyOperation : IServerOperation<DatabaseTopology>
    {
        private readonly string _database;

        public GetDatabaseTopologyOperation(string database)
        {
            _database = database;
        }

        public RavenCommand<DatabaseTopology> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetDatabaseTopologyCommand(ctx, _database);
        }
    }


    public class GetDatabaseTopologyCommand : RavenCommand<DatabaseTopology>
    {
        private readonly JsonOperationContext _ctx;
        private readonly string _database;
        private readonly DocumentConventions _conventions = new DocumentConventions();

        public override bool IsReadRequest => false;

        public GetDatabaseTopologyCommand(JsonOperationContext ctx, string database)
        {
            _ctx = ctx;
            _database = database;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/databases?name={_database}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            var rec = (DatabaseRecord)EntityToBlittable.ConvertToEntity(_ctx, 
                typeof(DatabaseRecord), "database-record", response, _conventions);
            Result = rec.Topology;
        }
    }
}
