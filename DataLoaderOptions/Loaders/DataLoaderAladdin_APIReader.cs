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
using CsvHelper;
using log4net;

namespace DataLoaderOptions
{
    class DataLoaderAladdin_APIReader : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 12;
        private static readonly ILog log = LogManager.GetLogger(Environment.MachineName);

        public DataLoaderAladdin_APIReader()
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
                FillDataTable(outputData, file);
                DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("InforceDate")).Max();

                //DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                //xl.SetHeaderRow(1, @"Buy/Sell");
                //var dateString = "1500-12-25";
                //var asOfDate = Extensions.Extensions.GetFirstDateFromString(dateString, @"\d{2}-[a-zA-Z]{3}-\d{4}$", "dd-MMM-yyyy") ?? throw new Exception();
                string outfile = "Positions-Numerix Input-from AladdinApiReader.csv";
                //xl.SetDataTableFormat(outputData);
                //outputData = xl.GetFilledDataTable(OnError.UseNullValue);


                if (outputData.Rows.Count > 0)
                {
                    outfile = OutputPath + asOfDate.ToString("yyyyMMdd") + "_" + outfile;
                    CorrectDataTable(outputData, outfile, asOfDate);
                    string filepath = OutputPath + Path.GetFileName(file);
                    string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        CheckSchema(con, outputData);
                    }
                    if (ToLoad)
                    {
                        lock (toLock)
                        {
                            base.LoadData(outputData);
                        }
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
                            log.Fatal("Error with DataLoader aladdin" + asOfDate);
                            Console.WriteLine(ex.Message);
#if DEBUG
                            throw ex;
#endif
                        }
                    }
                    if (File.Exists(outfile))
                    {
                        File.Delete(outfile);
                    }
                    File.Move(file, outfile);
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
            tbl.Columns.Add(@"Buy_Sell", typeof(string));
            tbl.Columns.Add(@"CUSIP_Aladdin_ID_", typeof(string));
            tbl.Columns.Add(@"Client_O", typeof(string));
            tbl.Columns.Add(@"Strategy", typeof(string));
            tbl.Columns.Add(@"Sec_Desc", typeof(string));
            tbl.Columns.Add(@"Strike_Price", typeof(decimal));
            tbl.Columns.Add(@"Maturity", typeof(DateTime));
            tbl.Columns.Add(@"Current_Face", typeof(decimal));
            tbl.Columns.Add(@"Trade_Date", typeof(DateTime));
            tbl.Columns.Add(@"REFERENCE_DATE", typeof(DateTime));
            tbl.Columns.Add(@"Counterparty_Full_Name", typeof(string));
            tbl.Columns.Add(@"SecDesc2", typeof(decimal));
            tbl.Columns.Add(@"Purchase_Price", typeof(decimal));
            tbl.Columns.Add(@"Initial_Level_Coupon", typeof(decimal));

            return tbl;
        }

        /// <summary>
        /// Fills the data table with the data found in the Aladdin CSV file which has been
        /// output from the AladdinApiReader.
        /// </summary>
        /// <param name="outputData">The output DataTable.</param>
        /// <param name="infile">The input file. Must be in CSV format.</param>
        protected void FillDataTable(DataTable outputData, string infile)
        {
            DateTime now = DateTime.UtcNow;
            using (TextReader reader = new StreamReader(infile))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    bool onFirstRow = true;
                    while (true)
                    {
                        string[] row = csv.Read();
                        if (onFirstRow)
                        {
                            onFirstRow = false;
                            continue;
                        }
                        if (row == null)
                        {
                            break;
                        }
                        DataRow toInsert = outputData.NewRow();
                        int i = 0;
                        foreach (DataColumn col in outputData.Columns)
                        {
                            if (row[i] != "")
                            {
                                toInsert[col.ColumnName] = Convert.ChangeType(row[i++], col.DataType);
                            }
                            else
                            {
                                i++;
                            }
                            if (i == row.Count())
                            {
                                break;
                            }
                        }
                        outputData.Rows.Add(toInsert);
                    }
                }
            }
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
                if (row["CUSIP_Aladdin_ID_"] != DBNull.Value && (string)row["CUSIP_Aladdin_ID_"] == "BGH4MRAT0")
                {
                    row["CUSIP_Aladdin_ID_"] = DateTime.Parse("2016-05-25");
                }
                if (row[@"Buy_Sell"].ToString().Contains("EXPIRE"))
                {
                    row.Delete();
                }
                //if (row[@"Buy_Sell"] == DBNull.Value)
                //{
                //    row.Delete();
                //}
                else
                {
                    //row["InforceDate"] = asOfDate;
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
            sqlBulkCopy.ColumnMappings.Add(@"LoadDate", @"LoadDate");
            sqlBulkCopy.ColumnMappings.Add(@"Source", @"Source");
            sqlBulkCopy.ColumnMappings.Add(@"UserId", @"UserId");
            sqlBulkCopy.ColumnMappings.Add(@"InforceDate", @"InforceDate");
            sqlBulkCopy.ColumnMappings.Add(@"Buy_Sell", @"Buy/Sell");
            sqlBulkCopy.ColumnMappings.Add(@"CUSIP_Aladdin_ID_", @"CUSIP(Aladdin ID)");
            sqlBulkCopy.ColumnMappings.Add(@"Client_O", @"Client O");
            sqlBulkCopy.ColumnMappings.Add(@"Strategy", @"Strategy");
            sqlBulkCopy.ColumnMappings.Add(@"Sec_Desc", @"Sec Desc");
            sqlBulkCopy.ColumnMappings.Add(@"Strike_Price", @"Strike Price");
            sqlBulkCopy.ColumnMappings.Add(@"Maturity", @"Maturity");
            sqlBulkCopy.ColumnMappings.Add(@"Current_Face", @"Current Face");
            sqlBulkCopy.ColumnMappings.Add(@"Trade_Date", @"Trade Date");
            sqlBulkCopy.ColumnMappings.Add(@"REFERENCE_DATE", @"REFERENCE_DATE");
            sqlBulkCopy.ColumnMappings.Add(@"Counterparty_Full_Name", @"Counterparty Full Name");
            sqlBulkCopy.ColumnMappings.Add(@"SecDesc2", @"SecDesc2");
            sqlBulkCopy.ColumnMappings.Add(@"Purchase_Price", @"Purchase Price"); 
            sqlBulkCopy.ColumnMappings.Add(@"Initial_Level_Coupon", @"Initial Level Coupon");
        }
    }
}
