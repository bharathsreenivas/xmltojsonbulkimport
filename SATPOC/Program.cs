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
            List<String> jsonDocuments = Utility.XmlToJSON(documentsToBeImported);
            await RunBulkImportAsync(jsonDocuments);

        }

        private async Task RunBulkImportAsync(List<String> documents)
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

            IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
            await bulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            for (int i = 0; i < numberOfBatches; i++)
            {
                // Generate JSON-serialized documents to import.

                int prefix = i * (int) numberOfDocumentsPerBatch;
                // Invoke bulk import API.

                var tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
                    do
                    {
                        try
                        {
                            bulkImportResponse = await bulkExecutor.BulkImportAsync(
                                documents: documents.GetRange(prefix, (int)numberOfDocumentsPerBatch),
                                enableUpsert: true,
                                disableAutomaticIdGeneration: true,
                                maxConcurrencyPerPartitionKeyRange: null,
                                maxInMemorySortingBatchSize: null,
                                cancellationToken: token);
                        }
                        catch (DocumentClientException de)
                        {
                            Trace.TraceError("Document client exception: {0}", de);
                            break;
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError("Exception: {0}", e);
                            break;
                        }
                    } while (bulkImportResponse.NumberOfDocumentsImported < documents.GetRange(prefix, (int)numberOfDocumentsPerBatch).Count);

                },
                token));


                await Task.WhenAll(tasks);
            }

            Trace.WriteLine("Overall summary:");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                totalNumberOfDocumentsInserted,
                Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
                totalTimeTakenSec));
            Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));
            Trace.WriteLine("--------------------------------------------------------------------- ");

            Trace.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }
    }
}

