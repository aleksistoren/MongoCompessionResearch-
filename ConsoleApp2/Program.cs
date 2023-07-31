using MongoDB.Driver;
using MongoDB.Bson;
using System;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Text;

public class Program
{
    public static void Main(string[] args)
    {
        string[] compressors = {"none", "snappy", "zlib", "zstd"};
        int[] sizes = {100, 1000, 2000, 5000, 10000}; // Insert sizes
        int numberOfQueries = 9;

        // Change this to your MongoDB host
        string host = "localhost:27017";

        double[,,] times = new double[sizes.Length, compressors.Length, numberOfQueries + 2];
        long[,,] sizesInBytes = new long[sizes.Length, compressors.Length, numberOfQueries + 2];
        
        var test = new TestQueries(null, sizes.Length, compressors.Length, numberOfQueries);

        for (var s = 0; s < sizes.Length; s++)
        {
            Console.WriteLine(s);
            var size = sizes[s];

            for (var c = 0; c < compressors.Length; c++)
            {
                var compressor = compressors[c];

                MongoClient client = new MongoClient(compressor == "none"
                    ? $"mongodb://{host}"
                    : $"mongodb://{host}/?compressors={compressor}");
                var db = client.GetDatabase("testdb");
                var collection = db.GetCollection<BsonDocument>("testcollection");

                test.collection = collection;

                test.InsertTestData(size, s, c);

                test.ExecuteAllQueries(s, c);

                test.ClearTestData();
            }
        }
        
        test.WriteResultsToCSV(sizes, compressors, numberOfQueries);

    }
}
