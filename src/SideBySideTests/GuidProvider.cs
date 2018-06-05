using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace SideBySideTests
{
    public interface IGuidProvider
    {
        IEnumerator<Guid> Guids();
    }

    class GuidProvider: IGuidProvider
    {
        private readonly List<Guid> guids = new List<Guid>();

        public IEnumerator<Guid> Guids()
        {
            return new Iterator(guids);
        }

        private class Iterator : IEnumerator<Guid>
        {
            private List<Guid> guids;
            private int current = 0;
            public Iterator(List<Guid> guids)
            {
                this.guids = guids ?? throw new ArgumentNullException(nameof(guids));
            }
            public Guid Current => guids[current];

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                while (current >= guids.Count)
                    guids.Add(Guid.NewGuid());
                return true;
            }

            public void Reset()
            {
                current = 0;
            }
        }
    }
}
