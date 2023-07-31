using System.Diagnostics;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;

public class TestQueries
{
    public IMongoCollection<BsonDocument> collection;
    private Stopwatch stopwatch;
    private double[,,] times;
    private long[,,] sizesInBytes;

    public TestQueries(IMongoCollection<BsonDocument> collection, int sizesLength, int compressorsLength,
        int numberOfQueries)
    {
        this.collection = collection;
        this.stopwatch = new Stopwatch();
        this.times = new double[sizesLength, compressorsLength, numberOfQueries+1];
        this.sizesInBytes = new long[sizesLength, compressorsLength, numberOfQueries+1];
    }

    public void InsertTestData(int size, int s, int c)
    {
        List<BsonDocument> customers = new List<BsonDocument>();
        for (int i = 0; i < size; i++)
        {
            var customer = new BsonDocument
            {
                {"Name", $"Customer {i}"},
                {
                    "Address", new BsonDocument
                    {
                        {"Street", $"Street {i}"},
                        {"City", $"City {i}"},
                        {"Country", $"Country {i}"}
                    }
                },
                {"OrderCount", i},
                {"TotalSpent", i * 100}
            };

            customers.Add(customer);
        }

        stopwatch.Restart();
        collection.InsertMany(customers);
        stopwatch.Stop();
        times[s, c, 0] = stopwatch.Elapsed.TotalSeconds;
        sizesInBytes[s, c, 0] = Encoding.UTF8.GetBytes(customers.ToJson()).Length;
    }

    private void ExecuteQuery(FilterDefinition<BsonDocument> filter, int s, int c, int q)
    {
        stopwatch.Restart();
        var result = collection.Find(filter).ToList();
        stopwatch.Stop();
        double time = stopwatch.Elapsed.TotalSeconds;
        long size = Encoding.UTF8.GetBytes(result.ToJson()).Length;

        times[s, c, q] = time;
        sizesInBytes[s, c, q] = size;
    }

    public void ExecuteAllQueries(int s, int c)
    {
        Query1(s, c, 1);
        Query2(s, c, 2);
        Query3(s, c, 3);
        Query4(s, c, 4);
        Query5(s, c, 5);
        Query6(s, c, 6);
        Query7(s, c, 7);
        Query8(s, c, 8);
        Query9(s, c, 9);
    }

    public void Query1(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Gt("OrderCount", 5000);
        ExecuteQuery(filter, s, c, q);
    }

    public void Query2(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("Address.Country", "Country 5000");
        ExecuteQuery(filter, s, c, q);
    }

    public void Query3(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Gt("TotalSpent", 500000);
        ExecuteQuery(filter, s, c, q);
    }

    public void Query4(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Regex("Address.City", new BsonRegularExpression("00$")),
            Builders<BsonDocument>.Filter.Gt("TotalSpent", 20000)
        );
        ExecuteQuery(filter, s, c, q);
    }

    public void Query5(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Gt("OrderCount", 7000);
        ExecuteQuery(filter, s, c, q);
    }

    public void Query6(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("Address.City", "City 5000");
        ExecuteQuery(filter, s, c, q);
    }

    public void Query7(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Gt("TotalSpent", 80000);
        ExecuteQuery(filter, s, c, q);
    }

    public void Query8(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Regex("Name", new BsonRegularExpression("Customer"));
        ExecuteQuery(filter, s, c, q);
    }

    public void Query9(int s, int c, int q)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("Address.Street", "Street 5000");
        ExecuteQuery(filter, s, c, q);
    }

    public void ClearTestData()
    {
        var filterDelete = Builders<BsonDocument>.Filter.Gte("OrderCount", 0);
        collection.DeleteMany(filterDelete);
    }
    
    public void WriteResultsToCSV(string filename, int[] sizes, string[] compressors, int numberOfQueries)
    {
        using (var writer = new StreamWriter(filename))
        {
            // Write headers
            writer.WriteLine("Size,Compression,Query,Time,Size,SavedTime,SavedTimePercentage");

            // Loop through the sizes
            for (int s = 0; s < sizes.Length; s++)
            {
                // Loop through the compressors
                for (int c = 0; c < compressors.Length; c++)
                {
                    // Loop through the queries
                    for (int q = 0; q < numberOfQueries; q++)
                    {
                        string size = sizes[s].ToString();
                        string compression = compressors[c];
                        string time = times[s, c, q].ToString();
                        string sizeInBytes = sizesInBytes[s, c, q].ToString();
                        double savedTime = times[s, 0, q] - times[s, c, q];
                        string savedTimeStr = savedTime.ToString();
                        double savedTimePercentage = (times[s, 0, q] != 0) ? (savedTime / times[s, 0, q]) * 100 : 0; // avoid division by zero
                        string savedTimePercentageStr = savedTimePercentage.ToString("F2"); // 2 decimal places

                        // Write row
                        writer.WriteLine($"{size},{compression},{q + 1},{time},{sizeInBytes},{savedTimeStr},{savedTimePercentageStr}");
                    }
                }
            }
        }
    }



    /*public void WriteResultsToCSV(int[] sizes, string[] compressors, int numberOfQueries)
    {
        using (StreamWriter file = new StreamWriter("results.csv"))
        {
            var baseColumns = new[]
            {
                "Size",
                "Compressor",
                "Total",
                "Total Saved (%)",
                "Total Size"
            };

            var queryColumns = Enumerable.Range(1, numberOfQueries)
                .SelectMany(i => new[]
                {
                    $"Query {i}",
                    $"Query {i} Saved (%)",
                    $"Query {i} Size"
                });

            var allColumns = baseColumns.Take(2)
                .Concat(queryColumns)
                .Concat(baseColumns.Skip(2));

            file.WriteLine(string.Join(",", allColumns));

            for (int s = 0; s < sizes.Length; s++)
            {
                int dataSize = sizes[s];

                for (int c = 0; c < compressors.Length; c++)
                {
                    string compressor = compressors[c];

                    double totalTime = 0;
                    double totalSaved = 0;
                    long totalSize = 0;

                    for (int q = 0; q < times.GetLength(2); q++)
                    {
                        totalTime += times[s, c, q];
                        totalSize += sizesInBytes[s, c, q];
                        totalSaved += compressor == "none" ? 0 : CalculateSaving(times, s, c, q);
                    }

                    StringBuilder line = GenerateLine(s, c, dataSize, compressor, totalTime, totalSaved, totalSize,
                        times, sizesInBytes);
                    file.WriteLine(line.ToString());
                }
            }
        }
    }

    private double CalculateSaving(double[,,] times, int dataSizeIndex, int compressorIndex, int queryIndex)
    {
        double originalTime = times[dataSizeIndex, 0, queryIndex];
        double compressedTime = times[dataSizeIndex, compressorIndex, queryIndex];
        return (originalTime - compressedTime) / originalTime * 100;
    }

    private StringBuilder GenerateLine(int dataSizeIndex, int compressorIndex, int dataSize, string compressor,
        double totalTime, double totalSaved, long totalSize, double[,,] times, long[,,] sizesInBytes)
    {
        StringBuilder line = new StringBuilder($"{dataSize},{compressor},");

        for (int q = 0; q < times.GetLength(2); q++)
        {
            line.Append($"{times[dataSizeIndex, compressorIndex, q]},");
        }

        line.Append($"{totalTime},");

        if (compressor == "none")
        {
            line.Append(Enumerable.Repeat(",", times.GetLength(2) - 1));
        }
        else
        {
            for (int q = 0; q < times.GetLength(2); q++)
            {
                line.Append($"{CalculateSaving(times, dataSizeIndex, compressorIndex, q)},");
            }

            line.Append($"{totalSaved},");
        }

        for (int q = 0; q < sizesInBytes.GetLength(2); q++)
        {
            line.Append($"{sizesInBytes[dataSizeIndex, compressorIndex, q]},");
        }

        line.Append($"{totalSize}");

        return line;
    }*/
}
