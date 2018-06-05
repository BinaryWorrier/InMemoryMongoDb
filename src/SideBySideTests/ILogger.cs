using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SideBySideTests
{
    public interface ILogger
    {
        void Debug(string text);
        void Info(string text);
        void Error(string text);
    }

    public class Logger : ILogger
    {
        private readonly List<TextWriter> console;
        private readonly List<TextWriter> error;

        public void AddConsole(TextWriter writer)
            => console.Add(writer);

        public void AddError(TextWriter writer)
            => error.Add(writer);

        public Logger()
        {
            console = new List<TextWriter> { System.Console.Out };
            error = new List<TextWriter> { System.Console.Error };
        }

        public sealed class ForeColor : IDisposable
        {
            private readonly ConsoleColor original;
            public ForeColor(ConsoleColor newColor)
            {
                original = Console.ForegroundColor;
                Console.ForegroundColor = newColor;
            }
            public void Dispose() => Console.ForegroundColor = original;
        }

        public void Debug(string text) => Write(console, "[Debug] " + text);

        public void Error(string text)
        {
            using (new ForeColor(ConsoleColor.Red))
                Write(error, "[Error] " + text);
        }

        public void Info(string text)
        {
            using (new ForeColor(ConsoleColor.Green))
                Write(console, "[Info ] " + text);
        }

        private void Write(List<TextWriter> writers, string text)
        {
            foreach (var writer in writers)
                writer.WriteLine(text);
        }
    }
}
