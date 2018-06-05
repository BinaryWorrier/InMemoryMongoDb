using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson;

namespace SideBySideTests
{
    public class Assert
    {
        private readonly ILogger logger;

        public Assert(ILogger logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void Equal(object actual, object expected)
        {
            if (!AreEqual(actual, expected))
                logger.Error($"Equal Failed: {actual} != {expected}");
        }
        public void NotEqual(object actual, object expected)
        {
            if (AreEqual(actual, expected))
                logger.Error($"NotEqual Failed: {actual} == {expected}");
        }

        private bool AreEqual(object actual, object expected)
        {
            if (actual == null && expected == null)
                return true;
            if (actual == null || expected == null)
                return false;

            return (expected is string exp && exp == actual?.ToString()) || actual.Equals(expected);
        }

        private string ShowNull(object actual) => actual == null ? "{null}" : actual.ToString();

        public void NotEqual<T>(T actual, T expected) where T : struct, IComparable<T>
        {
            if (actual.CompareTo(expected) == 0)
                logger.Error($"NotEqual Failed: {actual} == {expected}");
        }

        public void Equal<T>(T actual, T expected) where T: struct, IComparable<T>
        {
            if (actual.CompareTo(expected) != 0)
                logger.Error($"Equal Failed: {actual} != {expected}");
        }
    }
}
