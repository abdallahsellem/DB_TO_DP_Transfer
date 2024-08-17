
using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Threading.Tasks;
using DataTransferApp.Services;
using System.Configuration;
public class BulkTransfer
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<DataTransferService> _logger;

    public BulkTransfer(IConfiguration configuration, ILogger<DataTransferService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }
    public async Task TransferDataAsync(string tableName)
    {
        var sourceConnectionString = _configuration.GetConnectionString("SourceDatabase");
        var destinationConnectionString = _configuration.GetConnectionString("DestinationDatabase");

        using var sourceConnection = new MySqlConnection(sourceConnectionString);
        using var destinationConnection = new MySqlConnection(destinationConnectionString);

        await sourceConnection.OpenAsync();
        await destinationConnection.OpenAsync();

        // Read data from the source database
        var dataTable = await GetDataFromSourceAsync(sourceConnection, tableName);

        // Write data to the destination database
        await WriteDataToDestinationAsync(destinationConnection, tableName, dataTable);
    }

    private async Task<DataTable> GetDataFromSourceAsync(MySqlConnection connection, string tableName)
    {
        var dataTable = new DataTable();

        using var command = new MySqlCommand($"SELECT * FROM {tableName};", connection);
        using var reader = await command.ExecuteReaderAsync();

        dataTable.Load(reader);
        return dataTable;
    }

    private async Task WriteDataToDestinationAsync(MySqlConnection connection, string tableName, DataTable dataTable)
    {
        var columns = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(c => $"`{c.ColumnName}`"));
        var values = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(c => $"@{c.ColumnName}"));

        var insertQuery = $"INSERT INTO `{tableName}` ({columns}) VALUES ({values});";

        using var transaction = await connection.BeginTransactionAsync();
        using var command = new MySqlCommand(insertQuery, connection, transaction);

        foreach (DataRow row in dataTable.Rows)
        {
            command.Parameters.Clear();
            foreach (DataColumn column in dataTable.Columns)
            {
                command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
            }

            await command.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();
    }
}