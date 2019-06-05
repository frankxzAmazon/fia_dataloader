using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;
using System.Globalization;
using DataLoaderOptions.Readers;

namespace DataLoaderOptions
{
    class DataLoaderDLICRateSetting: DataLoader
    {
        string inputFolder;
        Dictionary<string, int> worksheet_header = new Dictionary<string, int>()
        {
            ["Target Income 10"] = 11,
            ["Chapters 10"] = 10,
            ["Stages 7"] = 10,
            ["Assured 7"] = 8,
            ["Renewals"] = 4
        };
        Dictionary<string, string> worksheet_product = new Dictionary<string, string>()
        {
            ["Target Income 10"] = "TI10",
            ["Chapters 10"] = "RC10",
            ["Stages 7"] = "RS7",
            ["Assured 7"] = "AI7"
        };
        static object toLock = new object();
        public DataLoaderDLICRateSetting()
        {
            inputFolder = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Rate Setting\ToUpload\";
            OutputPath = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Rate Setting\";
        }

        public override string SqlTableName => "DLIC.mapOptionBudget";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*.xlsx", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();
                DateTime asOfDate = Extensions.Extensions.GetFirstDateFromString(file, @"\b\d{2}-\d{4}\b", "MM-yyyy") ?? DateTime.MinValue;
                foreach (var ws in worksheet_header)
                {
                    DataReader_Excel xl = new DataReader_Excel(file, ws.Key,ws.Value);
                    xl.SetDataTableFormat(outputData);
                    try
                    {
                        outputData.Merge(xl.GetFilledDataTable(OnError.UseNullValue));
                        UpdateTable(outputData, asOfDate, ws.Key);
                    }
                    catch(System.Runtime.InteropServices.COMException ex)
                    {

                    }
                }

                if (outputData.Rows.Count > 0)
                {
                    
                    string fileName = OutputPath + Path.GetFileName(file);
                    if (!File.Exists(fileName))
                    {
                        
                        lock (toLock)
                        {
                            string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                            using (SqlConnection con = new SqlConnection(sqlString))
                            {
                                con.Open();
                                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                                {
                                    //Set the database table name
                                    sqlBulkCopy.DestinationTableName = SqlTableName;
                                    MapTable(sqlBulkCopy);
                                    sqlBulkCopy.WriteToServer(outputData);
                                }
                            }
                        }
                        if (File.Exists(fileName))
                        {
                            File.Delete(fileName);
                        }
                        File.Move(file, fileName);
                    }
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("Product", typeof(string));
            tbl.Columns.Add("Rate Cohort", typeof(DateTime));
            tbl.Columns.Add("Renewal Year", typeof(Int16));
            tbl.Columns.Add("Index", typeof(string));
            tbl.Columns.Add("Crediting Strategy", typeof(string));
            tbl.Columns.Add(@"Cap / Par/ Spread", typeof(decimal));
            tbl.Columns.Add("Option Price", typeof(decimal));
           
            return tbl;
        }
        protected void UpdateTable(DataTable outputData, DateTime asOfDate, string ws)
        {
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["Crediting Strategy"] == DBNull.Value || row["Index"] == DBNull.Value)
                {
                    row.Delete();
                }
                else
                {
                    if (row["Rate Cohort"] == DBNull.Value || row["Product"] == DBNull.Value)
                    {
                        row["Rate Cohort"] = asOfDate.AddMonths(1);
                        if (worksheet_product.ContainsKey(ws))
                        {
                            row["Product"] = worksheet_product[ws];
                        }
                        else
                        {
                            row.Delete();
                            continue;
                        }
                        row["Renewal year"] = 1;
                    }
                    if (row["Option Price"] == DBNull.Value)
                    {
                        row["Option Price"] = row[@"Cap / Par/ Spread"];
                    }
                }

            }
            outputData.AcceptChanges();
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("Product", "Product");
            sqlBulkCopy.ColumnMappings.Add("Rate Cohort", "RateCohort");
            sqlBulkCopy.ColumnMappings.Add("Renewal Year", "RenewalYear");
            
            sqlBulkCopy.ColumnMappings.Add("Index", "Index");
            sqlBulkCopy.ColumnMappings.Add("Crediting Strategy", "CreditingStrategy");
            sqlBulkCopy.ColumnMappings.Add(@"Cap / Par/ Spread", "Cap_Par_Spread");
            sqlBulkCopy.ColumnMappings.Add("Option Price", "Option_Price");
        }
    }
}
