using MongoDB.Driver;
using MongoDB.Driver.Core.Operations;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace InMemoryMongoDb
{
    static class INMAsyncCursor
    {
        public static IAsyncCursor<T> Create<T>(IEnumerable<T> seq)
            => new INMAsyncCursor<T>(seq);

        internal static IAsyncCursor<object> Create(object p)
        {
            throw new NotImplementedException();
        }
    }

    class INMAsyncCursor<T> : IAsyncCursor<T>
    {
        private static Task<bool> falseTask = Task.FromResult(false);
        private static Task<bool> trueTask = Task.FromResult(true);

        private static Task<bool> FromBool(bool value) => value ? trueTask : falseTask;

        private readonly IEnumerable<T> seq;
        private bool moveNext = true;
        public IEnumerable<T> Current => seq;
        public INMAsyncCursor(IEnumerable<T> seq)
        {
            this.seq = seq ?? throw new ArgumentNullException(nameof(seq));
        }

        public void Dispose()
        {

        }

        public bool MoveNext(CancellationToken cancellationToken = default)
        {
            var value = moveNext;
            moveNext = false;
            return value;
        }


        public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(MoveNext(cancellationToken));
    }
}
