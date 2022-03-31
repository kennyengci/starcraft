using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;

namespace StarCraft
{
    class Program
    {
        private static readonly HttpClient _client = new HttpClient();

        static void Main(string[] args)
        {
            var baseUrl = "https://irsa.ipac.caltech.edu/TAP/sync";
            int batchSize = 50000;

            // loop 200 times at batch size of 50k to get 10m rows
            for (int i = 0; i < 200; i++)
            {
                int startIndex = i * batchSize;
                int endIndex = (i + 1) * batchSize;

                var urlString = $"{baseUrl}?QUERY=SELECT%20%0ARANDOM_INDEX,%0ASOURCE_ID,%0Aastrometric_pseudo_colour,%0Ateff_val,%0Aradial_velocity,%0Aradius_val,%0Alum_val%20%0AFROM%20gaia_dr2_source%0A" +
                $"WHERE%20RANDOM_INDEX%20BETWEEN%20{startIndex}%20AND%20{endIndex}" +
                $"&FORMAT=csv";

                var stopWatch = new Stopwatch();
                stopWatch.Start();
                Console.WriteLine($"Starting request for rows between {startIndex} and {endIndex}");
                var response = _client.GetAsync(urlString).Result;
                stopWatch.Stop();
                Console.WriteLine($"Response received {stopWatch.Elapsed.TotalSeconds}");
                stopWatch.Reset();

                var records = response.Content.ReadAsStringAsync().Result;
                var recordList = records.Split("\n").Where(x => !string.IsNullOrWhiteSpace(x)).ToList();

                var csvRows = recordList.Select(x => x);
                var filePath = @"gaia.csv";

                if (i == 0)
                {
                    // write everything including the header the first time
                    File.WriteAllLines(filePath, csvRows);
                }
                else
                {
                    // skip the header
                    csvRows = csvRows.Skip(1);
                    File.AppendAllLines(filePath, csvRows);
                }
            }
        }
    }
}
