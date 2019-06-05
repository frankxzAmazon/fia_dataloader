using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions.Loaders
{
    class FixedWidthField_Decimal : IFixedWidthField
    {
        public FixedWidthField_Decimal(int startLocation, int width, int magnitude, string columnName)
        {
            StartLocation = startLocation;
            Width = width;
            Magnitude = (int)Math.Pow(10,magnitude);
            ColumnName = columnName;
        }
        public int StartLocation { get;  }
        public int Width { get;  }
        public int Magnitude { get;  }
        public Type DataType => typeof(decimal);
        public string ColumnName { get; }
        public object ReadValue(string newLine)
        {
            string value = newLine.Substring(StartLocation, Width);
            while(value.Length > 1 && value.Contains("-") && value.Substring(0,1) == "0")
            {
                value = value.Substring(1);
            }
            decimal ValueN = (decimal.Parse(value,
                 System.Globalization.NumberStyles.AllowParentheses |
                 System.Globalization.NumberStyles.AllowLeadingWhite |
                 System.Globalization.NumberStyles.AllowTrailingWhite |
                 System.Globalization.NumberStyles.AllowThousands |
                 System.Globalization.NumberStyles.AllowDecimalPoint |
                 System.Globalization.NumberStyles.AllowLeadingSign));
            return ValueN / Magnitude;
        }
    }
}
