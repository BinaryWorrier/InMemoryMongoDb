using MongoDB.Bson;
using System;

namespace SideBySideTests
{
    class Program
    {

        static void Main(string[] args)
        {

            var runner = new TestRunner();
            runner.Run();

            Console.WriteLine("Press any key");
            Console.ReadKey();

        }

    }
}
