using System;

namespace DataLoaderOptions
{
    interface IDataReader
    {
        string OutputPath { get; }
        void ReadData();
    }
}