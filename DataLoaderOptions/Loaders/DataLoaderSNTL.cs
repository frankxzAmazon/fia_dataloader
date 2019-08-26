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
    class DataLoaderSNTL : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 1;
        public DataLoaderSNTL()
        {
            inputFolder = @"W:\DACT\ALM\FIAHedging\DBUpload\SNTL\ToUpload\";
            OutputPath = @"W:\DACT\ALM\FIAHedging\DBUpload\SNTL\";
        }

        public override string SqlTableName => "GIS.PoliciesSNTL";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                string fileName = OutputPath + Path.GetFileName(file);
                DataTable outputData = CreateDataTable();
                DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                xl.SetDataTableFormat(outputData);
                outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                outputData.AcceptChanges();
                //var asOfDate = Extensions.Extensions.GetFirstDateFromString(file, @"\d{1,2}-\d{1,2}-\d{4}", "M-d-yyyy") ?? throw new Exception("Could not find date in string " + file);
                var asOfDate = Extensions.Extensions.GetFirstDateFromString(file, @"\d{8}", "MMddyyyy") ?? throw new Exception("Could not find date in string " + file);

                UpdateAsOfDate(outputData, asOfDate, fileName);
                outputData.AcceptChanges();
                lock (toLock)
                {
                    base.LoadData(outputData);
                }

                string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                if (ToLoad)
                {
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        using (SqlCommand cmd = new SqlCommand("GIS.InsertSntl", con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandTimeout = 0;
                            cmd.ExecuteNonQuery();
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
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("InforceDate", typeof(DateTime));
            tbl.Columns.Add("PolicyNumber", typeof(string));
            tbl.Columns.Add("IndexValue_PolicyDate", typeof(decimal));
            tbl.Columns.Add("AccountValue", typeof(decimal));
            tbl.Columns.Add("IndexCap", typeof(decimal));
            tbl.Columns.Add("ParticipationRate", typeof(decimal));
            tbl.Columns.Add("Fund", typeof(string));
            tbl.Columns.Add("RenewalDate", typeof(DateTime));
            tbl.Columns.Add("CoinsurancePercentage", typeof(decimal));
            tbl.Columns.Add("BudgetRate", typeof(decimal));
            tbl.Columns.Add("IssueDate", typeof(DateTime));
            tbl.Columns.Add("ValCode", typeof(string));


            return tbl;
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string source)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("Source", typeof(string));
            outputData.Columns.Add("UserID", typeof(string));
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["LoadDate"] = loadDate;
                row["UserID"] = Environment.UserName;
                row["InforceDate"] = asOfDate;
                row["Source"] = source;
            }
        }

        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("PolicyNumber", "PolicyNumber");
            sqlBulkCopy.ColumnMappings.Add("IndexValue_PolicyDate", "IndexValue_PolicyDate");
            sqlBulkCopy.ColumnMappings.Add("AccountValue", "AccountValue");
            sqlBulkCopy.ColumnMappings.Add("IndexCap", "IndexCap");
            sqlBulkCopy.ColumnMappings.Add("ParticipationRate", "ParticipationRate");
            sqlBulkCopy.ColumnMappings.Add("Fund", "Fund");
            sqlBulkCopy.ColumnMappings.Add("RenewalDate", "RenewalDate");
            sqlBulkCopy.ColumnMappings.Add("CoinsurancePercentage", "CoinsurancePercentage");
            sqlBulkCopy.ColumnMappings.Add("BudgetRate", "BudgetRate");
            sqlBulkCopy.ColumnMappings.Add("IssueDate", "IssueDate");
            sqlBulkCopy.ColumnMappings.Add("ValCode", "ValCode");


        }
    }
}
