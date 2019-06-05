using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions.Readers
{
    public class DataReader_SQL
    {
        public virtual DataTable GetFilledDataTable(OnError onError)
        {
            return new DataTable();
        }
    }
}
