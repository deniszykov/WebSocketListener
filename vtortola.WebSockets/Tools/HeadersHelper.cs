/*
	Copyright (c) 2017 Denis Zykov
	License: https://opensource.org/licenses/MIT
*/
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using JetBrains.Annotations;

namespace vtortola.WebSockets.Tools
{
    internal static class HeadersHelper
    {
        public static IEnumerable<KeyValuePair<string, string>> SplitAndTrimKeyValue([CanBeNull] string valueString, char valuesSeparator = ';', char nameValueSeparator = '=', StringSplitOptions options = StringSplitOptions.None)
        {
            if (valueString == null)
                yield break;

            var startIndex = 0;
            do
            {
                var nextValueIndex = valueString.IndexOf(valuesSeparator, startIndex);
                if (nextValueIndex < 0)
                    nextValueIndex = valueString.Length;

                var equalsIndex = valueString.IndexOf(nameValueSeparator, startIndex, nextValueIndex - startIndex);
                if (equalsIndex < 0)
                    equalsIndex = startIndex - 1;

                var nameStart = startIndex;
                var nameLength = Math.Max(0, equalsIndex - startIndex);

                TrimInPlace(valueString, ref nameStart, ref nameLength);

                var name = string.Empty;
                if (nameLength > 0)
                    name = valueString.Substring(nameStart, nameLength);

                var valueStart = equalsIndex + 1;
                var valueLength = nextValueIndex - equalsIndex - 1;
                var value = string.Empty;

                TrimInPlace(valueString, ref valueStart, ref valueLength);

                if (valueLength > 0)
                    value = valueString.Substring(valueStart, valueLength);
                else
                    value = string.Empty;

                if (options == StringSplitOptions.None || (string.IsNullOrWhiteSpace(value) == false || string.IsNullOrWhiteSpace(name) == false))
                    yield return new KeyValuePair<string, string>(name, value);

                startIndex = nextValueIndex + 1;
            } while (startIndex < valueString.Length);
        }
        
        public static void TrimInPlace([NotNull] string value, ref int startIndex, ref int length)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (startIndex < 0 || startIndex > value.Length) throw new ArgumentOutOfRangeException(nameof(startIndex));
            if (length < 0 || startIndex + length > value.Length) throw new ArgumentOutOfRangeException(nameof(length));

            if (length == 0)
                return;

            while (length > 0 && char.IsWhiteSpace(value[startIndex]))
            {
                startIndex++;
                length--;
            }

            if (length == 0) return;
            var end = startIndex + length - 1;
            while (length > 0 && char.IsWhiteSpace(value[end]))
            {
                end--;
                length--;
            }
        }
        public static void Skip(string value, ref int startIndex, UnicodeCategory category)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (startIndex < 0 || startIndex > value.Length) throw new ArgumentOutOfRangeException(nameof(startIndex));


            while (startIndex < value.Length && CharUnicodeInfo.GetUnicodeCategory(value[startIndex]) == category)
                startIndex++;
        }
    }
}
