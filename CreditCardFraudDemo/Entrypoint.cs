using Amazon.Lambda.Core;
using Amazon.SageMakerRuntime;
using Amazon.SageMakerRuntime.Model;
using Amazon.SimpleNotificationService;
using BAMCIS.AWSLambda.Common;
using BAMCIS.AWSLambda.Common.Events.KinesisFirehose;
using BAMCIS.AWSLambda.Common.SageMaker;
using BAMCIS.AWSLambda.Common.SageMaker.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.XRay.Recorder.Core;
using Amazon.XRay.Recorder.Handlers.AwsSdk;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BAMCIS.LambdaFunctions.CreditCardFraudDemo
{
    public class Entrypoint
    {
        #region Private Fields

        private static IAmazonSageMakerRuntime _SagemakerClient;
        private static IAmazonSimpleNotificationService _SNSClient;
        private static Random _Rand;
        private static string _SNS_FAILURE_TOPIC_ARN;

        private static readonly double[][] _Locations = new double[][] {
            new[] { 38.969555, -77.386098 },  // Herndon, VA
            new[] { 39.290385, -76.612189 },  // Baltimore, MD
            new[] { 32.776664, -96.796988 },  // Dallas, TX
            new[] { 32.715738, -117.161084 }, // San Diego, CA
            new[] { 36.153982, -95.992775 },  // Tulsa, OK
            new[] { 34.746481, -92.289595 },  // Little Rock, AR
            new[] { 28.538335, -81.379236 },  // Orlando, FL
            new[] { 35.227087, -80.843127 },  // Charlotte, NC
            new[] { 32.776475, -79.931051 },  // Charleston, SC
            new[] { 42.331427, -83.045754 },  // Detroit, MI
            new[] { 41.878114, -87.629798 },  // Chicago, IL
            new[] { 37.774929, -122.419416 }, // San Francisco, CA
            new[] { 29.760427, -95.369803 },  // Houston, TX
            new[] { 35.686975, -105.937799 }, // Santa Fe, NM
            new[] { 32.222607, -110.974711 }, // Tucson, AZ
            new[] { 51.507351, -0.127758 },   // London, UK
            new[] { 48.856614, 2.352222 },    // Paris, FR
            new[] { 51.339695, 12.373075 },   // Leipzig, DE
            new[] { 41.902783, 12.496366 },   // Rome
            new[] { 53.349805, -6.260310 }    // Dublin, IR
        };

        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor to initialize static fields
        /// </summary>
        static Entrypoint()
        {
            AWSSDKHandler.RegisterXRayForAllServices();

            _SagemakerClient = new AmazonSageMakerRuntimeClient();
            _SNSClient = new AmazonSimpleNotificationServiceClient();
            _Rand = new Random();
            _SNS_FAILURE_TOPIC_ARN = Environment.GetEnvironmentVariable("SNS_FAILURE_TOPIC_ARN");           
        }

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Entrypoint()
        {       
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Processes the kinesis firehose records from a kinesis stream
        /// </summary>
        /// <param name="request"></param>
        /// <returns>The transformed records</returns>
        public async Task<KinesisFirehoseTransformResponse> Exec(KinesisFirehoseEvent<KinesisFirehoseKinesisStreamRecord> request, ILambdaContext context)
        {
            context.LogInfo($"Received firehose transform request for {request.Records.Count()} records.");

            List<KinesisFirehoseTransformedRecord> TransformedRecords = new List<KinesisFirehoseTransformedRecord>();

            string Endpoint = Environment.GetEnvironmentVariable("SageMakerEndpoint");
            string SNSTopic = Environment.GetEnvironmentVariable("SNS");

            Func<string, Task<TransformationResult>> Transform = new Func<string, Task<TransformationResult>>(async data =>
            {
                // The data is json
                try
                {
                    JObject Obj = JObject.Parse(data);
                    string Row = String.Join(",", ((JArray)Obj["transactionDetails"]).Select(x => (double)x)) + "\n";

                    using (MemoryStream MStream = new MemoryStream(Encoding.UTF8.GetBytes(Row)))
                    {
                        InvokeEndpointRequest Request = new InvokeEndpointRequest()
                        {
                            Accept = "application/json",
                            ContentType = "text/csv",
                            EndpointName = Endpoint,
                            Body = MStream,
                        };

                        InvokeEndpointResponse Response = await _SagemakerClient.InvokeEndpointAsync(Request);

                        using (StreamReader Reader = new StreamReader(Response.Body))
                        {
                            LinearLearnerBinaryInferenceResponse Json = JsonConvert.DeserializeObject<LinearLearnerBinaryInferenceResponse>(Reader.ReadToEnd());
                            BinaryClassificationPrediction Prediction = Json.Predictions.FirstOrDefault();

                            if (Prediction != null)
                            {
                                Row = Row.TrimEnd();
                                double[] Location = _Locations[_Rand.Next(0, _Locations.Length - 1)];
                                Row += $",{Location[0]},{Location[1]},{Prediction.PredictedLabel},{Prediction.Score}\n";

                                if (Prediction.PredictedLabel == 1)
                                {
                                    context.LogWarning($"FRAUD ALERT DETECTED: {Row}");

                                    if (!String.IsNullOrEmpty(SNSTopic))
                                    {
                                        await _SNSClient.PublishAsync(SNSTopic, $"FRAUD ALERT DETECTED: {Row}", "FRAUD ALERT!!!");
                                    }
                                }

                                return new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes(Row)), TransformationResultStatus.OK);
                            }
                            else
                            {
                                return new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes("No predictions were returned")), TransformationResultStatus.PROCESSING_FAILED);
                            }
                        }
                    }
                }
                catch (AggregateException e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e.InnerException, context);

                    return new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes(e.InnerException.Message)), TransformationResultStatus.PROCESSING_FAILED);
                }
                catch (Exception e)
                {
                    context.LogError(e);

                    await SendFailureSNS(e, context);

                    return new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes(e.Message)), TransformationResultStatus.PROCESSING_FAILED);
                }
            });

            foreach (KinesisFirehoseRecord Record in request.Records)
            {
                try
                {
                    KinesisFirehoseTransformedRecord TransformedRecord = await KinesisFirehoseTransformedRecord.BuildAsync(Record, Transform);
                    TransformedRecords.Add(TransformedRecord);
                }
                catch (AggregateException e)
                {
                    context.LogError($"Failed while processing record {Record.RecordId}.", e);

                    KinesisFirehoseTransformedRecord TransformedRecord = KinesisFirehoseTransformedRecord.Build(Record, x => new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes(e.InnerException.Message)), TransformationResultStatus.PROCESSING_FAILED));
                    TransformedRecords.Add(TransformedRecord);

                    await SendFailureSNS(e.InnerException, context);
                }
                catch (Exception e)
                {
                    context.LogError($"Failed while processing record {Record.RecordId}.", e);

                    KinesisFirehoseTransformedRecord TransformedRecord = KinesisFirehoseTransformedRecord.Build(Record, x => new TransformationResult(Convert.ToBase64String(Encoding.UTF8.GetBytes(e.Message)), TransformationResultStatus.PROCESSING_FAILED));
                    TransformedRecords.Add(TransformedRecord);

                    await SendFailureSNS(e, context);
                }
            }

            context.LogInfo($"Completed transform.");

            return new KinesisFirehoseTransformResponse(TransformedRecords);
        }

        #endregion

        #region Private Methods

        private static async Task SendFailureSNS(string message, ILambdaContext context)
        {
            if (!String.IsNullOrEmpty(_SNS_FAILURE_TOPIC_ARN))
            {
                await _SNSClient.PublishAsync(_SNS_FAILURE_TOPIC_ARN, message, $"Lambda Execution Failure: {context.InvokedFunctionArn}");
            }
        }

        private static async Task SendFailureSNS(Exception e, ILambdaContext context)
        {
            await SendFailureSNS($"{e.Message}\n{e.StackTrace}", context);
        }

        #endregion
    }
}
