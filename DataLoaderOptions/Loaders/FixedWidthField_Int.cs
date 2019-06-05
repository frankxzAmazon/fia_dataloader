using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions.Loaders
{
    class FixedWidthField_Int : IFixedWidthField
    {
        public FixedWidthField_Int(int startLocation, int width, string columnName)
        {
            StartLocation = startLocation;
            Width = width;
            ColumnName = columnName;
        }
        public int StartLocation { get;  }
        public int Width { get;  }
        public Type DataType => typeof(int);
        public string ColumnName { get; }
        public object ReadValue(string newLine)
        {
            return Convert.ToInt32(newLine.Substring(StartLocation, Width));
        }
    }
}
