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
        double totalTime = 0;
        int iterations = 10;
        long size = 0;

        for (int i = 0; i < iterations; i++)
        {
            stopwatch.Restart();
            var result = collection.Find(filter).ToList();
            stopwatch.Stop();

            double time = stopwatch.Elapsed.TotalSeconds;
            size = Encoding.UTF8.GetBytes(result.ToJson()).Length;

            totalTime += time;
        }

        double averageTime = totalTime / iterations;

        times[s, c, q] = averageTime;
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
            var headers =
                new StringBuilder("Size,Compression,InsertTime,InsertSize,InsertSavedTime,InsertSavedTimePercentage,");
            for (int q = 1; q < numberOfQueries; q++)
            {
                headers.Append($"Query{q}Time,Query{q}Size,Query{q}SavedTime,Query{q}SavedTimePercentage,");
            }

            headers.Append("TotalSavedTimePercentage");
            writer.WriteLine(headers);

            // Loop through the sizes
            for (int s = 0; s < sizes.Length; s++)
            {
                // Loop through the compressors
                for (int c = 0; c < compressors.Length; c++)
                {
                    var totalOriginalTime = 0.0;
                    var totalCurrentTime = 0.0;

                    var line = new StringBuilder($"{sizes[s]},{compressors[c]},");

                    // Special handling for insert time (q=0)
                    string insertTime = times[s, c, 0].ToString();
                    string insertSize = sizesInBytes[s, c, 0].ToString();
                    double insertSavedTime = times[s, 0, 0] - times[s, c, 0];
                    double insertSavedTimePercentage =
                        (times[s, 0, 0] != 0) ? (insertSavedTime / times[s, 0, 0]) * 100 : 0; // avoid division by zero
                    line.Append(
                        $"{insertTime},{insertSize},{insertSavedTime.ToString()},{insertSavedTimePercentage.ToString("F2")},");

                    totalOriginalTime += times[s, 0, 0];
                    totalCurrentTime += times[s, c, 0];

                    // Loop through the queries (q starts from 1)
                    for (int q = 1; q < numberOfQueries; q++)
                    {
                        string time = times[s, c, q].ToString();
                        string sizeInBytes = sizesInBytes[s, c, q].ToString();
                        double savedTime = times[s, 0, q] - times[s, c, q];
                        string savedTimeStr = savedTime.ToString();
                        double savedTimePercentage =
                            (times[s, 0, q] != 0) ? (savedTime / times[s, 0, q]) * 100 : 0; // avoid division by zero
                        string savedTimePercentageStr = savedTimePercentage.ToString("F2"); // 2 decimal places

                        line.Append($"{time},{sizeInBytes},{savedTimeStr},{savedTimePercentageStr},");

                        totalOriginalTime += times[s, 0, q];
                        totalCurrentTime += times[s, c, q];
                    }

                    double totalSavedTimePercentage = (totalOriginalTime != 0)
                        ? ((totalOriginalTime - totalCurrentTime) / totalOriginalTime) * 100
                        : 0; // avoid division by zero
                    string totalSavedTimePercentageStr = totalSavedTimePercentage.ToString("F2"); // 2 decimal places

                    line.Append(totalSavedTimePercentageStr);

                    // Write row
                    writer.WriteLine(line);
                }
            }
        }
    }
}
