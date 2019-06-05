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
using Extensions;

namespace DataLoaderOptions
{
    class DataLoaderGISAssets : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 4;

        public DataLoaderGISAssets()
        {
            inputFolder = @"W:\DACT\ALM\FIAHedging\DBUpload\GIS Assets\ToUpload\"; 
            OutputPath = @"W:\DACT\ALM\FIAHedging\DBUpload\GIS Assets\";
        }

        public override string SqlTableName => "GIS.InvDailyOptionDataReport";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();
                DataReader_Excel xl;
                string fileName;
                if (file.CaseInsensitiveContains("inv", StringComparison.InvariantCultureIgnoreCase))
                {
                    xl = new DataReader_Excel(file, headerRow);
                    fileName = "INV Daily Option Data.xlsx";
                }
                else
                {
                    xl = new DataReader_Excel(file, 1);
                    fileName = "Daily Option Data.xlsx";
                }
                xl.SetDataTableFormat(outputData);
                outputData = xl.GetFilledDataTable(OnError.UseNullValue);

                CorrectDataTable(outputData, file);
                outputData.AcceptChanges();
                if (outputData.Rows.Count > 0)
                {
                    DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("As Of Date")).Max();
                    fileName = OutputPath + asOfDate.ToString("yyyyMMdd") + "_" + fileName;
                    string filepath = OutputPath + Path.GetFileName(file);
                    outputData.AcceptChanges();
                    string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        CheckSchema(con, outputData);
                    }
                    lock (toLock)
                    {
                        base.LoadData(outputData);
                    }
                    if (ToLoad)
                    {
                        try
                        {
                            using (SqlConnection con = new SqlConnection(sqlString))
                            {
                                con.Open();
                                using (SqlCommand cmd = new SqlCommand("GIS.InsertAssets", con))
                                {
                                    cmd.CommandType = CommandType.StoredProcedure;
                                    cmd.CommandTimeout = 0;
                                    cmd.ExecuteNonQuery();
                                }
                                con.Close();
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
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
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("LoadDate", typeof(DateTime));
            tbl.Columns.Add("Source", typeof(string));
            tbl.Columns.Add("UserId", typeof(string));
            tbl.Columns.Add("As of Date", typeof(DateTime));
            tbl.Columns.Add("As of Market Price", typeof(DateTime));
            tbl.Columns.Add("Portfolio Short Name", typeof(string));
            tbl.Columns.Add("CUSIP", typeof(string));
            tbl.Columns.Add("Issue Date Of Sec", typeof(DateTime));
            tbl.Columns.Add("Counterparty Broker", typeof(string));
            tbl.Columns.Add("SM Description", typeof(string));
            tbl.Columns.Add("SM2 Desc Class Cap", typeof(decimal));
            tbl.Columns.Add("PM Bucket", typeof(string));
            tbl.Columns.Add("Option Strike Price", typeof(decimal));
            tbl.Columns.Add("Maturity Date", typeof(DateTime));
            tbl.Columns.Add("BRS Current Face",typeof(decimal));
            tbl.Columns.Add("BRS Full Market Value",typeof(decimal));
            tbl.Columns.Add("BRS Purchase Price", typeof(decimal));
            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string source)
        {
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["LoadDate"] = loadDate;
                row["UserID"] = Environment.UserName;
                row["Source"] = source;
                if (row["SM2 Desc Class Cap"] == DBNull.Value)
                {
                    row["SM2 Desc Class Cap"] = 0;
                }
                if (row["CUSIP"] != DBNull.Value && (string)row["CUSIP"] == "BGH4MRAT0")
                {
                    row["Issue Date Of Sec"] = DateTime.Parse("2016-05-25");
                }
                if (row["As of Date"] == DBNull.Value || row["Option Strike Price"] == DBNull.Value || row["Maturity Date"] == DBNull.Value || row["Issue Date Of Sec"] == DBNull.Value)
                {
                    row.Delete();
                }

            }
            outputData.AcceptChanges();
        }
        public void CheckSchema(SqlConnection conn, DataTable dt)
        {
            if (conn.State == ConnectionState.Closed) conn.Open();
            var table = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter("select top 0 * from " + SqlTableName, conn);
            da.FillSchema(table, SchemaType.Source);
            foreach (var row in dt.AsEnumerable().ToList())
            {
                foreach (DataColumn col in row.Table.Columns)
                {
                    if(table.Columns.Contains(col.ColumnName))
                    {
                        if(!table.Columns[col.ColumnName].AllowDBNull && row[col.ColumnName] == DBNull.Value)
                        {
                            if (col.ColumnName == "PM Bucket")
                            {
                                row[col] ="Derivatives.US Dollar.Options";
                            }
                            else
                            {
                                row.Delete();
                                break;
                            }
                        }
                    }
                }
            }
            dt.AcceptChanges();
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("As of Date", "As of Date");
            sqlBulkCopy.ColumnMappings.Add("As of Market Price", "As of Market Price");
            sqlBulkCopy.ColumnMappings.Add("Portfolio Short Name", "Portfolio Short Name");
            sqlBulkCopy.ColumnMappings.Add("CUSIP", "CUSIP");
            sqlBulkCopy.ColumnMappings.Add("Issue Date Of Sec", "Issue Date Of Sec");
            sqlBulkCopy.ColumnMappings.Add("SM Description", "SM Description");
            sqlBulkCopy.ColumnMappings.Add("Counterparty Broker", "Counterparty Broker");
            sqlBulkCopy.ColumnMappings.Add("SM2 Desc Class Cap", "SM2 Desc Class Cap");
            sqlBulkCopy.ColumnMappings.Add("PM Bucket", "PM Bucket");
            sqlBulkCopy.ColumnMappings.Add("Option Strike Price", "Option Strike Price");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "Maturity Date");
            sqlBulkCopy.ColumnMappings.Add("BRS Current Face","BRS Current Face");
            sqlBulkCopy.ColumnMappings.Add("BRS Full Market Value","BRS Full Market Value");
            sqlBulkCopy.ColumnMappings.Add("BRS Purchase Price", "BRS Purchase Price");
          
        }
    }
}
