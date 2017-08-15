using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ETL;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Tcp;
using Sparrow.Json;

namespace Raven.Client.Json.Converters
{
    internal class JsonDeserializationClient : JsonDeserializationBase
    {        
        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, IsDatabaseLoadedCommand.CommandResult> IsDatabaseLoadedCommandResult = GenerateJsonDeserializationRoutine<IsDatabaseLoadedCommand.CommandResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetConflictsResult.Conflict> DocumentConflict = GenerateJsonDeserializationRoutine<GetConflictsResult.Conflict>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetConflictsResult> GetConflictsResult = GenerateJsonDeserializationRoutine<GetConflictsResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, AttachmentDetails> AttachmentDetails = GenerateJsonDeserializationRoutine<AttachmentDetails>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, AttachmentName> AttachmentName = GenerateJsonDeserializationRoutine<AttachmentName>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, QueryResult> QueryResult = GenerateJsonDeserializationRoutine<QueryResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, MoreLikeThisQueryResult> MoreLikeThisQueryResult = GenerateJsonDeserializationRoutine<MoreLikeThisQueryResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, Topology> Topology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ClusterTopologyResponse> ClusterTopology = GenerateJsonDeserializationRoutine<ClusterTopologyResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, NodeInfo> NodeInfo = GenerateJsonDeserializationRoutine<NodeInfo>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, TcpConnectionHeaderResponse> TcpConnectionHeaderResponse = GenerateJsonDeserializationRoutine<TcpConnectionHeaderResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, DatabasePutResult> DatabasePutResult = GenerateJsonDeserializationRoutine<DatabasePutResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ModifyOngoingTaskResult> ModifyOngoingTaskResult = GenerateJsonDeserializationRoutine<ModifyOngoingTaskResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OngoingTaskSubscription> GetOngoingTaskSubscriptionResult = GenerateJsonDeserializationRoutine<OngoingTaskSubscription>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OngoingTaskReplication> GetOngoingTaskReplicationResult = GenerateJsonDeserializationRoutine<OngoingTaskReplication>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OngoingTaskRavenEtl> GetOngoingTaskRavenEtlResult = GenerateJsonDeserializationRoutine<OngoingTaskRavenEtl>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OngoingTaskBackup> GetOngoingTaskBackupResult = GenerateJsonDeserializationRoutine<OngoingTaskBackup>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OngoingTaskSqlEtl> GetOngoingTaskSqlEtlResult = GenerateJsonDeserializationRoutine<OngoingTaskSqlEtl>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ModifySolverResult> ModifySolverResult = GenerateJsonDeserializationRoutine<ModifySolverResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, DisableDatabaseToggleResult> DisableResourceToggleResult = GenerateJsonDeserializationRoutine<DisableDatabaseToggleResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, BlittableArrayResult> BlittableArrayResult = GenerateJsonDeserializationRoutine<BlittableArrayResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PutTransformerResult> PutTransformerResult = GenerateJsonDeserializationRoutine<PutTransformerResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, DatabaseStatistics> GetStatisticsResult = GenerateJsonDeserializationRoutine<DatabaseStatistics>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, OperationIdResult> OperationIdResult = GenerateJsonDeserializationRoutine<OperationIdResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, HiLoResult> HiLoResult = GenerateJsonDeserializationRoutine<HiLoResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, TcpConnectionInfo> TcpConnectionInfo = GenerateJsonDeserializationRoutine<TcpConnectionInfo>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, SubscriptionConnectionServerMessage> SubscriptionNextObjectResult = GenerateJsonDeserializationRoutine<SubscriptionConnectionServerMessage>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, CreateSubscriptionResult> CreateSubscriptionResult = GenerateJsonDeserializationRoutine<CreateSubscriptionResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetSubscriptionsResult> GetSubscriptionsResult = GenerateJsonDeserializationRoutine<GetSubscriptionsResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, FacetedQueryResult> FacetedQueryResult = GenerateJsonDeserializationRoutine<FacetedQueryResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, SuggestionQueryResult> SuggestQueryResult = GenerateJsonDeserializationRoutine<SuggestionQueryResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, TermsQueryResult> TermsQueryResult = GenerateJsonDeserializationRoutine<TermsQueryResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, IndexingStatus> IndexingStatus = GenerateJsonDeserializationRoutine<IndexingStatus>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetIndexesResponse> GetIndexesResponse = GenerateJsonDeserializationRoutine<GetIndexesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetTransformersResponse> GetTransformersResponse = GenerateJsonDeserializationRoutine<GetTransformersResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetIndexNamesResponse> GetIndexNamesResponse = GenerateJsonDeserializationRoutine<GetIndexNamesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetTransformerNamesResponse> GetTransformerNamesResponse = GenerateJsonDeserializationRoutine<GetTransformerNamesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetIndexStatisticsResponse> GetIndexStatisticsResponse = GenerateJsonDeserializationRoutine<GetIndexStatisticsResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PutIndexesResponse> PutIndexesResponse = GenerateJsonDeserializationRoutine<PutIndexesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, IndexErrors> IndexErrors = GenerateJsonDeserializationRoutine<IndexErrors>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PatchResult> PatchResult = GenerateJsonDeserializationRoutine<PatchResult>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetCertificatesResponse> GetCertificatesResponse = GenerateJsonDeserializationRoutine<GetCertificatesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetClientCertificatesResponse> GetClientCertificatesResponse = GenerateJsonDeserializationRoutine<GetClientCertificatesResponse>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, BuildNumber> BuildNumber = GenerateJsonDeserializationRoutine<BuildNumber>();

        public static readonly Func<JsonOperationContext, BlittableJsonReaderObject, SubscriptionState> SubscriptionState = GenerateJsonDeserializationRoutine<SubscriptionState>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ExceptionDispatcher.ExceptionSchema> ExceptionSchema = GenerateJsonDeserializationRoutine<ExceptionDispatcher.ExceptionSchema>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, DeleteDatabaseResult> DeleteDatabaseResult = GenerateJsonDeserializationRoutine<DeleteDatabaseResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ConfigureExpirationOperationResult> ConfigureExpirationOperationResult = GenerateJsonDeserializationRoutine<ConfigureExpirationOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, UpdatePeriodicBackupOperationResult> ConfigurePeriodicBackupOperationResult = GenerateJsonDeserializationRoutine<UpdatePeriodicBackupOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetPeriodicBackupStatusOperationResult> GetPeriodicBackupStatusOperationResult = GenerateJsonDeserializationRoutine<GetPeriodicBackupStatusOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, RestoreBackupOperationResult> RestoreResultOperationResult = GenerateJsonDeserializationRoutine<RestoreBackupOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PeriodicBackupStatus> PeriodicBackupStatus = GenerateJsonDeserializationRoutine<PeriodicBackupStatus>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ConfigureRevisionsOperationResult> ConfigureRevisionsOperationResult = GenerateJsonDeserializationRoutine<ConfigureRevisionsOperationResult>();
        
        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ExternalReplication> ExternalReplication = GenerateJsonDeserializationRoutine<ExternalReplication>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ModifyCustomFunctionsResult> ModifyCustomFunctionResult = GenerateJsonDeserializationRoutine<ModifyCustomFunctionsResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, AddEtlOperationResult> AddEtlOperationResult = GenerateJsonDeserializationRoutine<AddEtlOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, UpdateEtlOperationResult> UpdateEtlOperationResult = GenerateJsonDeserializationRoutine<UpdateEtlOperationResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, EtlProcessState> EtlProcessState = GenerateJsonDeserializationRoutine<EtlProcessState>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, PutConnectionStringResult> PutConnectionStringResult = GenerateJsonDeserializationRoutine<PutConnectionStringResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, RemoveConnectionStringResult> RemoveConnectionStringResult = GenerateJsonDeserializationRoutine<RemoveConnectionStringResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetConnectionStringsResult> GetConnectionStringsResult = GenerateJsonDeserializationRoutine<GetConnectionStringsResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, SmugglerResult> SmugglerResult = GenerateJsonDeserializationRoutine<SmugglerResult>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, ClientConfiguration> ClientConfiguration = GenerateJsonDeserializationRoutine<ClientConfiguration>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GetClientConfigurationOperation.Result> ClientConfigurationResult = GenerateJsonDeserializationRoutine<GetClientConfigurationOperation.Result>();                

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, S3Settings> S3Settings = GenerateJsonDeserializationRoutine<S3Settings>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, GlacierSettings> GlacierSettings = GenerateJsonDeserializationRoutine<GlacierSettings>();

        internal static readonly Func<JsonOperationContext, BlittableJsonReaderObject, AzureSettings> AzureSettings = GenerateJsonDeserializationRoutine<AzureSettings>();
    }
}
