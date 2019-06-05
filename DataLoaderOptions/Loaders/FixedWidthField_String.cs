using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions.Loaders
{
    class FixedWidthField_String : IFixedWidthField
    {
        public FixedWidthField_String(int startLocation, int width, string columnName)
        {
            StartLocation = startLocation;
            Width = width;
            ColumnName = columnName;
        }
        public int StartLocation { get;  }
        public int Width { get;  }
        public Type DataType => typeof(string);
        public string ColumnName { get; }
        public object ReadValue(string newLine)
        {
            return newLine.Substring(StartLocation, Width);
        }
    }
}
