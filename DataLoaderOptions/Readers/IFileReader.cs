using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions.Readers
{
    public interface IFileReader
    {
        string FilePath { get; set; }
        bool ContainsHeaders { get; }
        int HeaderRow { get; }
        void SetDataTableFormat(DataTable dt);
        DataTable GetFilledDataTable(OnError onError);
    }
}
