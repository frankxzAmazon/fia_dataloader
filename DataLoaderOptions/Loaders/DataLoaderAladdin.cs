using DataLoaderOptions.Readers;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace DataLoaderOptions
{
    class DataLoaderAladdin : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 12;

        public DataLoaderAladdin()
        {
            inputFolder = @"W:\DACT\ALM\FIAHedging\DBUpload\Aladdin\ToUpload\";
            OutputPath = @"W:\DACT\ALM\FIAHedging\DBUpload\Aladdin\";
        }

        public override string SqlTableName => "dbo.AladdinOptionInventory";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {

                DataTable outputData = CreateDataTable();

                DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                xl.SetHeaderRow(1, @"Buy/Sell");
                var dateString = (string)xl.GetCell(4, 1);
                var asOfDate = Extensions.Extensions.GetFirstDateFromString(dateString, @"\d{2}-[a-zA-Z]{3}-\d{4}$", "dd-MMM-yyyy") ?? throw new Exception();
                string fileName = "Positions-Numerix Input.xlsx";
                xl.SetDataTableFormat(outputData);
                outputData = xl.GetFilledDataTable(OnError.UseNullValue);


                if (outputData.Rows.Count > 0)
                {
                    fileName = OutputPath + asOfDate.ToString("yyyyMMdd") + "_" + fileName;
                    CorrectDataTable(outputData, fileName, asOfDate);
                    string filepath = OutputPath + Path.GetFileName(file);
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
                                using (SqlCommand cmd = new SqlCommand("dbo.InsertAladdin", con))
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
            tbl.Columns.Add(@"LoadDate", typeof(DateTime));
            tbl.Columns.Add(@"Source", typeof(string));
            tbl.Columns.Add(@"UserId", typeof(string));
            tbl.Columns.Add(@"InforceDate", typeof(DateTime));
            tbl.Columns.Add(@"Buy/Sell", typeof(string));
            tbl.Columns.Add(@"CUSIP(Aladdin ID)", typeof(string));
            tbl.Columns.Add(@"Client O", typeof(string));
            tbl.Columns.Add(@"Strategy", typeof(string));
            tbl.Columns.Add(@"Sec Desc", typeof(string));
            tbl.Columns.Add(@"Strike Price", typeof(decimal));
            tbl.Columns.Add(@"Maturity", typeof(DateTime));
            tbl.Columns.Add(@"Current Face", typeof(decimal));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"REFERENCE_DATE", typeof(DateTime));
            tbl.Columns.Add(@"Counterparty Full Name", typeof(string));
            tbl.Columns.Add(@"SecDesc2", typeof(decimal));
            tbl.Columns.Add(@"Purchase Price", typeof(decimal));

            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string source, DateTime asOfDate)
        {
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["LoadDate"] = loadDate;
                row["UserID"] = Environment.UserName;
                row["Source"] = source;
                if (row["SecDesc2"] == DBNull.Value)
                {
                    row["SecDesc2"] = 0;
                }
                if (row["CUSIP(Aladdin ID)"] != DBNull.Value && (string)row["CUSIP(Aladdin ID)"] == "BGH4MRAT0")
                {
                    row["CUSIP(Aladdin ID)"] = DateTime.Parse("2016-05-25");
                }
                if (row[@"Buy/Sell"] == DBNull.Value)
                {
                    row.Delete();
                }
                else
                {
                    row["InforceDate"] = asOfDate;
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
                    if (table.Columns.Contains(col.ColumnName))
                    {
                        if (!table.Columns[col.ColumnName].AllowDBNull && row[col.ColumnName] == DBNull.Value)
                        {
                            if (col.ColumnName == "PM Bucket")
                            {
                                row[col] = "Derivatives.US Dollar.Options";
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
            sqlBulkCopy.ColumnMappings.Add(@"LoadDate", @"LoadDate");
            sqlBulkCopy.ColumnMappings.Add(@"Source", @"Source");
            sqlBulkCopy.ColumnMappings.Add(@"UserId", @"UserId");
            sqlBulkCopy.ColumnMappings.Add(@"InforceDate", @"InforceDate");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", @"Buy/Sell");
            sqlBulkCopy.ColumnMappings.Add(@"CUSIP(Aladdin ID)", @"CUSIP(Aladdin ID)");
            sqlBulkCopy.ColumnMappings.Add(@"Client O", @"Client O");
            sqlBulkCopy.ColumnMappings.Add(@"Strategy", @"Strategy");
            sqlBulkCopy.ColumnMappings.Add(@"Sec Desc", @"Sec Desc");
            sqlBulkCopy.ColumnMappings.Add(@"Strike Price", @"Strike Price");
            sqlBulkCopy.ColumnMappings.Add(@"Maturity", @"Maturity");
            sqlBulkCopy.ColumnMappings.Add(@"Current Face", @"Current Face");
            sqlBulkCopy.ColumnMappings.Add(@"Trade Date", @"Trade Date");
            sqlBulkCopy.ColumnMappings.Add(@"REFERENCE_DATE", @"REFERENCE_DATE");
            sqlBulkCopy.ColumnMappings.Add(@"Counterparty Full Name", @"Counterparty Full Name");
            sqlBulkCopy.ColumnMappings.Add(@"SecDesc2", @"SecDesc2");
            sqlBulkCopy.ColumnMappings.Add(@"Purchase Price", @"Purchase Price");


        }
    }
}
