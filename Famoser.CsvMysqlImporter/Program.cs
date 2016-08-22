using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Nito.AsyncEx;

namespace Famoser.CsvMysqlImporter
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncContext.Run(() => MainAsync(args));
        }

        private static async void MainAsync(string[] args)
        {
            Console.WriteLine("press q to quit");
            while (true)
            {
                try
                {
                    Console.WriteLine("connection string: ");
                    var connString = Console.ReadLine();
                    Console.WriteLine("table name: ");
                    var tableName = Console.ReadLine();
                    Console.WriteLine("skip table creation (y/n)");
                    var skipTable = Console.ReadLine() == "y";
                    Console.WriteLine("path of .csv: ");
                    var path = Console.ReadLine();
                    Console.WriteLine("reading in csv file...");
                    var lines = File.ReadAllLines(path);
                    using (var conn = new MySqlConnection(connString))
                    {
                        conn.Open();

                        var header = lines[0].Split(new[] { "," }, StringSplitOptions.None);
                        for (int i = 0; i < header.Length; i++)
                        {
                            header[i] = header[i].Replace(" ", "");
                        }

                        if (skipTable)
                        {
                            var comm = conn.CreateCommand();
                            var fields = new List<string>()
                            {
                                "id INT(12) UNSIGNED AUTO_INCREMENT PRIMARY KEY"
                            };
                            foreach (var s in header)
                            {
                                fields.Add(s + " TEXT");
                            }

                            comm.CommandText = "CREATE TABLE " + tableName + " (" + string.Join(",", fields) + ")";
                            await comm.ExecuteNonQueryAsync();
                            Console.WriteLine("created table");
                        }

                        var insertFields = string.Join(",", header);
                        for (int i = 1; i < lines.Length; i++)
                        {
                            var com = conn.CreateCommand();
                            com.CommandText = "INSERT INTO " + tableName + " (" + insertFields + ") VALUES (" + lines[i] + ")";
                            await com.ExecuteNonQueryAsync();

                            Console.WriteLine("inserted " + i + " of " + i + " entries");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("exception occurred: " + ex);
                }
            }

        }
    }
}
