using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataLoaderOptions.Readers;
using System.Data;
using System.Data.SqlClient;

namespace DataLoaderOptions.Monitors
{
    public class SQLMonitor
    {
        //private SqlDependency watcher = new SqlDependency();
        //public string TableName { get; set; }
        //public SqlConnection Connection { get; set; }
        //public DataReader_SQL Reader { get; set; }
        //public Action<DataTable> ActionOnChange { get; set; }

        //public void BeginMonitoring()
        //{
        //    if (Connection != null && Connection.State == ConnectionState.Closed)
        //    {
        //        SqlDependency.Start(Connection.ConnectionString);
        //        string sqlCommand = "Select * from " + TableName;
        //        SqlCommand command = new SqlCommand(sqlCommand, Connection);
        //        watcher.AddCommandDependency(command);
        //        watcher.OnChange += OnChange;
                
        //    }

        //}
        //public void CancelMonitoring()
        //{
            
        //}
        //private void OnChange(object sender,SqlNotificationEventArgs args)
        //{
        //    var dataTable = Reader.GetFilledDataTable(OnError.ResumeNext);
        //    ActionOnChange.Invoke(dataTable);
        //}
    }
}
