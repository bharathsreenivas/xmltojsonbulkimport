using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.Azure.Documents;

namespace SATPOC
{
    class ConsoleHelper
    {
        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);
        private static readonly string InputFolder = ConfigurationManager.AppSettings["InputFolder"];

        private static readonly ConnectionPolicy ConnectionPolicyDirect = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 10000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        private DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private ConsoleHelper(DocumentClient client)
        {
            this.client = client;
        }

        public static void Main(string[] args)
        {
              try
            {
                using (var client = new DocumentClient(
                    new Uri(EndpointUrl),
                    AuthorizationKey,
                    ConnectionPolicyDirect))
                {
                    var consoleHelper = new ConsoleHelper(client);
                    consoleHelper.ProcessTasks().GetAwaiter().GetResult();
                    Console.WriteLine("Sample completed successfully.");
                }
            }

            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadLine();
            }
        }

        private async Task ProcessTasks()
        {
            List<String> documentsToBeImported = new List<string>();
            FileReader reader = new FileReader(InputFolder);
            documentsToBeImported = reader.LoadDocuments();
            List<Tuple<String, String, String>> mapping = Utility.XmlToTuples(documentsToBeImported);
            List<GremlinEdge> edges = new List<GremlinEdge>();
            List<GremlinVertex> vertices = new List<GremlinVertex>();

            foreach (var tuple in mapping)
            {
                string outvertex = tuple.Item1;
                string invertex = tuple.Item2;
                string type = tuple.Item3;

                if (type == "E")
                {
                    GremlinVertex vertex1 = new GremlinVertex(outvertex, "Emisor");
                    vertex1.AddProperty("pk", outvertex);
                    GremlinVertex vertex2 = new GremlinVertex(invertex, "CFDI");
                    vertex2.AddProperty("pk", invertex);
                    if (!vertices.Contains(vertex1))
                    {
                        vertices.Add(vertex1);
                    }

                    if (!vertices.Contains(vertex2))
                    {
                        vertices.Add(vertex2);
                    }

                    GremlinEdge edge = new GremlinEdge(outvertex + invertex, outvertex + invertex, outvertex, invertex, outvertex, invertex, outvertex, invertex);
                    if (!edges.Contains(edge))
                    {
                        edges.Add(edge);
                    }
                }
                else if (type == "R")
                {
                    GremlinVertex vertex1 = new GremlinVertex(outvertex, "CFDI");
                    vertex1.AddProperty("pk", outvertex);
                    GremlinVertex vertex2 = new GremlinVertex(invertex, "Receptor");
                    vertex2.AddProperty("pk", invertex);
                    if (!vertices.Contains(vertex1))
                    {
                        vertices.Add(vertex1);
                    }

                    if (!vertices.Contains(vertex2))
                    {
                        vertices.Add(vertex2);
                    }

                    GremlinEdge edge = new GremlinEdge(outvertex + invertex, outvertex + invertex, outvertex, invertex, outvertex, invertex, outvertex, invertex);
                    if (!edges.Contains(edge))
                    {
                        edges.Add(edge);
                    }
                }
            }

            await RunBulkImportAsync(edges, vertices);

        }

        private async Task RunBulkImportAsync(List<GremlinEdge> edges, List<GremlinVertex> vertices)
        {
            // Cleanup on start if set in config.

            DocumentCollection dataCollection = null;
            try
            {
                if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]))
                {
                    Database database = CosmosDBAccessor.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Trace.TraceInformation("Creating database {0}", DatabaseName);
                    database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                    Trace.TraceInformation(String.Format("Creating collection {0} with {1} RU/s", CollectionName, CollectionThroughput));
                    dataCollection = await CosmosDBAccessor.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName, CollectionThroughput);
                }
                else
                {
                    dataCollection = CosmosDBAccessor.GetCollectionIfExists(client, DatabaseName, CollectionName);
                    if (dataCollection == null)
                    {
                        throw new Exception("The data collection does not exist");
                    }
                }
            }
            catch (Exception de)
            {
                Trace.TraceError("Unable to initialize, exception message: {0}", de.Message);
                throw;
            }

            // Prepare for bulk import.

            long numberOfDocumentsToGenerate = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToImport"]);
            int numberOfBatches = int.Parse(ConfigurationManager.AppSettings["NumberOfBatches"]);
            long numberOfDocumentsPerBatch = (long)Math.Floor(((double)numberOfDocumentsToGenerate) / numberOfBatches);

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;


            IBulkExecutor graphbulkExecutor = new GraphBulkExecutor(client, dataCollection);
            await graphbulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            BulkImportResponse vResponse = null;
            BulkImportResponse eResponse = null;

            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource(); 
            var token = tokenSource.Token;

            try
            {
                vResponse = await graphbulkExecutor.BulkImportAsync(
                        vertices,
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxConcurrencyPerPartitionKeyRange: null,
                        maxInMemorySortingBatchSize: null,
                        cancellationToken: token);

                eResponse = await graphbulkExecutor.BulkImportAsync(
                        edges,
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxConcurrencyPerPartitionKeyRange: null,
                        maxInMemorySortingBatchSize: null,
                        cancellationToken: token);
            }
            catch (DocumentClientException de)
            {
                Trace.TraceError("Document client exception: {0}", de);
            }
            catch (Exception e)
            {
                Trace.TraceError("Exception: {0}", e);
            }

            Console.WriteLine("\nSummary for batch");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(
                "Inserted {0} graph elements ({1} vertices, {2} edges) @ {3} writes/s, {4} RU/s in {5} sec)",
                vResponse.NumberOfDocumentsImported + eResponse.NumberOfDocumentsImported,
                vResponse.NumberOfDocumentsImported,
                eResponse.NumberOfDocumentsImported,
                Math.Round(
                    (vResponse.NumberOfDocumentsImported) /
                    (vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds)),
                Math.Round(
                    (vResponse.TotalRequestUnitsConsumed + eResponse.TotalRequestUnitsConsumed) /
                    (vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds)),
                vResponse.TotalTimeTaken.TotalSeconds + eResponse.TotalTimeTaken.TotalSeconds);
            Console.WriteLine(
                "Average RU consumption per insert: {0}",
                (vResponse.TotalRequestUnitsConsumed + eResponse.TotalRequestUnitsConsumed) /
                (vResponse.NumberOfDocumentsImported + eResponse.NumberOfDocumentsImported));
            Console.WriteLine("---------------------------------------------------------------------\n ");

            if (vResponse.BadInputDocuments.Count > 0 || eResponse.BadInputDocuments.Count > 0)
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@".\BadVertices.txt", true))
                {
                    foreach (object doc in vResponse.BadInputDocuments)
                    {
                        file.WriteLine(doc);
                    }
                }

                using (System.IO.StreamWriter file = new System.IO.StreamWriter(@".\BadEdges.txt", true))
                {
                    foreach (object doc in eResponse.BadInputDocuments)
                    {
                        file.WriteLine(doc);
                    }
                }
            }

            // Cleanup on finish if set in config.
            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Trace.TraceInformation("Deleting Database {0}", DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }



            Trace.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }
    }
}

