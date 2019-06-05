using System;

namespace DataLoaderOptions.Loaders
{
    interface IFixedWidthField
    {
        string ColumnName { get;  }
        int StartLocation { get;  }
        int Width { get;  }
        Type DataType { get; }
        object ReadValue(string newLine);
    }
}