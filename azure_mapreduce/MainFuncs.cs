using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage.Blob;

namespace azure_mapreduce
{
    public static class MainFuncs
    {
        [FunctionName("MainFuncs")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // parse query parameter
            string name = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
                .Value;

            // Get request body
            dynamic data = await req.Content.ReadAsAsync<object>();

            // Set name to query string or body data
            name = name ?? data?.name;

            return name == null
                ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
                : req.CreateResponse(HttpStatusCode.OK, "Hello " + name);
        }

        [FunctionName("MapReduceOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStartProcess(
           [HttpTrigger(AuthorizationLevel.Function, "get", "post")]
                 HttpRequestMessage req,
           [OrchestrationClient] DurableOrchestrationClient starter,
           TraceWriter log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("MapReduceOrchestrator", null);
            log.Info($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }







        [FunctionName("MapReduceOrchestrator")]
        public static async Task<int> MapReduceOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context)
        {
            var mapContainers = (containerMapIn: "datain", containerMapOut: "dataout");
            var reduceContainer = "dataout";

            // Start the mapping sub-orchestrator
            await context.CallSubOrchestratorAsync("MapOrchestrator", mapContainers);

            // Start the reduce sub orchestrator
            var maximumTemperature = await context.CallSubOrchestratorAsync<int>("ReduceOrchestrator", reduceContainer);

            return maximumTemperature;
        }

        [FunctionName("MapOrchestrator")]
        public static async Task MapOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context)
        {
            (string containerIn, string containerOut) mapContainers = context.GetInput<(string, string)>();

            var mapTasks = new List<Task>();

            // Get list of files to process from storage
            var fileList = await context.CallActivityAsync<List<Uri>>("GetFilesFromStorage", mapContainers.containerIn);

            // Process mapping with file content.
            foreach (var file in fileList)
            {
                var mapInfo = new
                {
                    fileName = Path.GetFileName(file.AbsoluteUri),
                    containerMapIn = mapContainers.containerIn,
                    containerMapOut = mapContainers.containerOut
                };

                mapTasks.Add(context.CallActivityAsync("MapDataFromFile", mapInfo));
            }

            await Task.WhenAll(mapTasks);
        }

        [FunctionName("MapDataFromFile")]
        [StorageAccount("DataStorage")]
        public static async Task MapDataFromFile(
           [ActivityTrigger] JObject input,
           [Blob("{input.containerMapIn}/{input.fileName}", FileAccess.Read)] Stream inputStream,
           [Blob("{input.containerMapOut}/{input.fileName}", FileAccess.Write)] Stream outputStream)
        {
            using (var reader = new StreamReader(inputStream))
            using (var writer = new StreamWriter(outputStream))
            {
                await reader.ReadLineAsync(); // read header row

                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    var year = MapYear(line);
                    var temperature = MapTemperature(line);

                    if (temperature != null)
                        await writer.WriteLineAsync($"{year},{temperature}");
                }
            }
        }

        public static string MapYear(string inputLine)
        {
            return inputLine.Substring(15, 4);
        }

        public static string MapTemperature(string inputLine)
        {
            string temp;

            if (inputLine[87] == '+')
                temp = inputLine.Substring(88, 4);
            else
                temp = inputLine.Substring(87, 5);

            if (temp == "9999") // value missing
                temp = null;

            return temp;
        }

        [FunctionName("ReduceOrchestrator")]
        public static async Task<int> ReduceOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context)
        {
            string containerIn = context.GetInput<string>();

            var resultTasks = new List<Task<int>>();

            // Get list of files to process from storage
            var fileList = await context.CallActivityAsync<List<Uri>>("GetFilesFromStorage", containerIn);

            // Process mapping with file content.
            foreach (var file in fileList)
            {
                var reduceInfo = new
                {
                    fileName = Path.GetFileName(file.AbsoluteUri),
                    containerReduceIn = containerIn
                };

                resultTasks.Add(context.CallActivityAsync<int>("ReduceDataFromFile", reduceInfo));
            }

            var highValues = await Task.WhenAll(resultTasks);
            return highValues.Max();
        }

        [FunctionName("ReduceDataFromFile")]
        [StorageAccount("DataStorage")]
        public static async Task<int> ReduceDataFromFile(
           [ActivityTrigger] JObject input,
           [Blob("{input.containerReduceIn}/{input.fileName}", FileAccess.Read)] Stream inputStream)
        {
            int maxTemp = 0;
            using (var reader = new StreamReader(inputStream))
            {
                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    var values = line.Split(',');
                    var temperature = Int32.Parse(values[1]) / 10;
                    if (temperature > maxTemp)
                        maxTemp = temperature;
                }
            }

            return maxTemp;
        }

        [FunctionName("GetFilesFromStorage")]
        [StorageAccount("DataStorage")]
        public static async Task<List<Uri>> GetFilesFromStorage(
           [ActivityTrigger] string containerName,
           [Blob("{containerName}", FileAccess.Read)] CloudBlobContainer container)
        {
            var list = await container.ListBlobsSegmentedAsync(new BlobContinuationToken());
            return list.Results.Select(b => b.Uri).ToList();
        }

    }
}
