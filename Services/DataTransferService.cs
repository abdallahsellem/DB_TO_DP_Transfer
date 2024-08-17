using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Threading.Tasks;

namespace DataTransferApp.Services
{
    public class DataTransferService
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<DataTransferService> _logger;

        public DataTransferService(IConfiguration configuration, ILogger<DataTransferService> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public async Task TransferDataAsync()
        {
            var sourceConnectionString = _configuration.GetConnectionString("SourceDatabase");
            var destinationConnectionString = _configuration.GetConnectionString("DestinationDatabase");

            using var sourceConnection = new MySqlConnection(sourceConnectionString);
            using var destinationConnection = new MySqlConnection(destinationConnectionString);

            try
            {
                await sourceConnection.OpenAsync();
                await destinationConnection.OpenAsync();

                var tables = await sourceConnection.QueryAsync<string>("SHOW TABLES;");

                foreach (var table in tables)
                {
                    var data = await sourceConnection.QueryAsync($"SELECT * FROM {table};");



                    if (data != null)
                    {
                        var batchSize = 1000; // Adjust based on performance testing
                        var rows = new List<string>();
                        var columnNames = string.Join(",", ((IDictionary<string, object>)data.First()).Keys);
                        foreach (var row in data)
                        {
                            var columnValues = string.Join(",", ((IDictionary<string, object>)row).Values.Select(value =>
                            {
                                if (value is string || value is DateTime)
                                {
                                    return $"'{value.ToString().Replace("'", "''")}'";
                                }
                                else if (value == null)
                                {
                                    return "NULL";
                                }
                                else
                                {
                                    return value.ToString();
                                }
                            }));
                            rows.Add($"({columnValues})");

                            if (rows.Count >= batchSize)
                            {
                                using var transaction = await destinationConnection.BeginTransactionAsync();
                                var insertQuery = $"INSERT INTO {table} ({columnNames}) VALUES {string.Join(",", rows)};";
                                using var command = new MySqlCommand(insertQuery, destinationConnection, transaction);
                                object value = await command.ExecuteNonQueryAsync();
                                rows.Clear();
                                await transaction.CommitAsync();

                            }
                        }
                        if (rows.Count > 0)
                        {
                            var insertQuery = $"INSERT INTO {table} ({columnNames}) VALUES {string.Join(",", rows)};";
                            await destinationConnection.ExecuteAsync(insertQuery);
                        }



                    }

                }

                _logger.LogInformation("Data transfer successful.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Data transfer failed.");
                throw;
            }
            finally
            {
                await sourceConnection.CloseAsync();
                await destinationConnection.CloseAsync();
            }
        }
    }
}
