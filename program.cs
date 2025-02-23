using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dapper;
using Confluent.Kafka;
using Newtonsoft.Json;  // Ensure Newtonsoft.Json is installed

namespace SQL_CDC_with_Kafka
{
    class Program
    {
        private const string SqlConnectionString = "<Your connection string>";
        private const string KafkaBootstrapServers = "localhost:9092";
        private const string KafkaTopic = "cdc-data-topic";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting SQL CDC to Kafka...");

            var producerConfig = new ProducerConfig { BootstrapServers = KafkaBootstrapServers };

            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            while (true)
            {
                try
                {
                    using var connection = new SqlConnection(SqlConnectionString);
                    await connection.OpenAsync();

                    var changes = await connection.QueryAsync<CdcChange>(@"
    SELECT 
        CT.__$operation AS OperationCode,
        CT.Id, 
        CT.Name, 
        CT.CreatedAt, 
        CONVERT(BIGINT, CT.__$seqval) AS SeqVal
    FROM cdc.CdcChange_CDC_CT CT
    WHERE CONVERT(BIGINT, CT.__$seqval) > ISNULL((SELECT MAX(SeqVal) FROM [dbo].[CdcChange]), 0)
    ORDER BY CT.__$seqval ASC;");

                    foreach (var change in changes)
                    {
                        // Map CDC operation type codes to human-readable names
                        string operationType = change.OperationCode switch
                        {
                            1 => "INSERT",
                            2 => "UPDATE (Before)",
                            3 => "UPDATE (After)",
                            4 => "DELETE",
                            _ => "UNKNOWN"
                        };

                        var messageObj = new
                        {
                            Operation = operationType,
                            change.OperationCode,
                            change.Id,
                            change.Name,
                            change.CreatedAt
                        };

                        string message = JsonConvert.SerializeObject(messageObj);

                        await producer.ProduceAsync(KafkaTopic, new Message<string, string>
                        {
                            Key = change.Id.ToString(),
                            Value = message
                        });

                        Console.WriteLine($"Sent to Kafka: {message}");

                        // ✅ Store last processed CDC sequence number in CdcChange, NOT CdcChange
                        await connection.ExecuteAsync(
                            "INSERT INTO CdcChange (SeqVal) VALUES (@SeqVal)",
                            new { change.SeqVal });
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }

                await Task.Delay(5000); // Check for new changes every 5 seconds
            }
        }
    }

    public class CdcChange
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime CreatedAt { get; set; }
        public long SeqVal { get; set; }
        public int OperationCode { get; set; }  // Stores the raw CDC operation code
    }
}
