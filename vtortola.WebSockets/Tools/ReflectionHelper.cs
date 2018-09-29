using System;
using System.Linq.Expressions;

namespace vtortola.WebSockets.Tools
{
    internal sealed class ReflectionHelper
    {
        public static readonly bool IsDynamicCompilationSupported;

        static ReflectionHelper()
        {
            try
            {
                IsDynamicCompilationSupported = Expression.Lambda<Func<bool>>(Expression.Constant(true)).Compile().Invoke();
            }
            catch
            {
                IsDynamicCompilationSupported = false;
            }
        }

        public static TypeCode GetTypeCode(Type type)
        {
            if (type == null) return TypeCode.Empty;
            if (type == typeof(bool)) return TypeCode.Boolean;
            if (type == typeof(char)) return TypeCode.Char;
            if (type == typeof(sbyte)) return TypeCode.SByte;
            if (type == typeof(byte)) return TypeCode.Byte;
            if (type == typeof(short)) return TypeCode.Int16;
            if (type == typeof(ushort)) return TypeCode.UInt16;
            if (type == typeof(int)) return TypeCode.Int32;
            if (type == typeof(uint)) return TypeCode.UInt32;
            if (type == typeof(long)) return TypeCode.Int64;
            if (type == typeof(ulong)) return TypeCode.UInt64;
            if (type == typeof(float)) return TypeCode.Single;
            if (type == typeof(double)) return TypeCode.Double;
            if (type == typeof(decimal)) return TypeCode.Decimal;
            if (type == typeof(DateTime)) return TypeCode.DateTime;
            if (type == typeof(string)) return TypeCode.String;

            return TypeCode.Object;
        }
    }
}
