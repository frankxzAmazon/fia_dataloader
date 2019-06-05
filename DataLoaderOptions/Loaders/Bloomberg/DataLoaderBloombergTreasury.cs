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
using CsvHelper;
using Extensions;

namespace DataLoaderOptions
{
    class DataLoaderBloombergTreasury : DataLoader
    {

        string inputFolder;
        string[] tenors = new string[] { "1M", "2M", "3M", "6M", "1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y", "8Y", "9Y", "10Y", "15Y", "20Y", "25Y", "30Y" };
        static object toLock = new object();

        public DataLoaderBloombergTreasury()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\";
        }

        public override string SqlTableName => "dbo.tblTreasuryRates";
        public async Task LoadData(int waitTimeInMinutes)
        {
            Task t2 = Task.Run(() => LoadToSql());
            await Task.WhenAll(t2);
        }
        public override void LoadToSql()
        {
            lock (toLock)
            {
                string[] files = Directory.GetFiles(inputFolder.ToString(), "*TreasCurveFnl*", SearchOption.AllDirectories);
                foreach (string file in files)
                {
                    DataTable outputData = CreateDataTable();
                    DateTime asOfDate = Extensions.Extensions.GetFirstDateFromString(Path.GetFileName(file), @"\d{8}", "yyyyMMdd") ?? throw new Exception();
                    if (asOfDate >= new DateTime(2018, 11, 5))
                    {
                        FillDataTable(outputData, GetConnString(file, true), asOfDate);
                        string sqlString = ConfigurationManager.ConnectionStrings["MarketData"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            foreach (DataRow row in outputData.AsEnumerable().ToList())
                            {
                                SqlCommand checkRowExists = new SqlCommand("SELECT COUNT(*) FROM " + SqlTableName + " WHERE [ticker] = @ticker and [ValDate] = @valdate", con);
                                checkRowExists.Parameters.AddWithValue("@ticker", row["Ticker"]);
                                checkRowExists.Parameters.AddWithValue("@valdate", row["Valdate"]);
                                int count = (int)checkRowExists.ExecuteScalar();
                                if (count > 0 || row["Valdate"] == DBNull.Value)
                                {
                                    row.Delete();
                                }
                            }
                            outputData.AcceptChanges();
                            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                            {
                                //Set the database table name
                                sqlBulkCopy.BulkCopyTimeout = 0;
                                sqlBulkCopy.DestinationTableName = SqlTableName;
                                MapTable(sqlBulkCopy);
                                sqlBulkCopy.WriteToServer(outputData);
                            }
                            con.Close();
                        }
                        string fileName = OutputPath + Path.GetFileName(file);
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
            tbl.Columns.Add("ValDate", typeof(DateTime));
            tbl.Columns.Add("Ticker", typeof(string));
            tbl.Columns.Add("Tenor", typeof(string));
            tbl.Columns.Add("Ask", typeof(decimal));
            tbl.Columns.Add("Mid", typeof(decimal));
            tbl.Columns.Add("Bid", typeof(decimal));
            tbl.Columns.Add("Coupon", typeof(decimal));
            tbl.Columns.Add("BT", typeof(char));
            tbl.Columns.Add("MaturityDate", typeof(DateTime));
            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            //sqlBulkCopy.ColumnMappings.Add("idChangeLog", "idChangeLog");
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Ticker", "Ticker");
            sqlBulkCopy.ColumnMappings.Add("Tenor", "Tenor");
            sqlBulkCopy.ColumnMappings.Add("Ask", "Ask");
            sqlBulkCopy.ColumnMappings.Add("Mid", "Mid");
            sqlBulkCopy.ColumnMappings.Add("Bid", "Bid");
            sqlBulkCopy.ColumnMappings.Add("Coupon", "Coupon");
            sqlBulkCopy.ColumnMappings.Add("MaturityDate", "MaturityDate");
            sqlBulkCopy.ColumnMappings.Add("BT", @"B/T");
        }
        protected void FillDataTable(DataTable outputData, string conString, DateTime asOfDate)
        {
            var listDetail = new List<TreasuryDetail>();
            using (TextReader reader = new StreamReader(conString))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    csv.Configuration.Delimiter = "|";
                    while (true)
                    {
                        string[] row = csv.Read();
                        if (row == null)
                        {
                            break;
                        }
                        if (row[0].Equals("START-OF-DATA", StringComparison.InvariantCultureIgnoreCase))
                        {
                            while (true)
                            {
                                row = csv.Read();
                                if (row == null || row[0].Equals("END-OF-DATA", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    break;
                                }
                                var detail = new TreasuryDetail();
                                detail.ValDate = asOfDate;
                                detail.Ticker = row[0];
                                detail.BT = Convert.ToChar(row[3]);
                                detail.MaturityDate = Convert.ToDateTime(row[4]);
                                detail.Coupon = Convert.ToDecimal(row[5] == "N.S." ? "0" : row[5]) / 100;
                                detail.Bid = Convert.ToDecimal(row[6] == "N.S." ? "0" : row[6]) / 100;
                                detail.Ask = Convert.ToDecimal(row[7] == "N.S." ? "0" : row[7]) / 100;
                                detail.Mid = Convert.ToDecimal(row[8] == "N.S." ? "0" : row[8]) / 100;
                                listDetail.Add(detail);
                            }
                        }
                    }
                }
            }
            int i = 0;
            foreach(var trsy in listDetail.OrderBy(x=> x.MaturityDate))
            {
                DataRow toInsert = outputData.NewRow();
                toInsert["ValDate"] = trsy.ValDate;
                toInsert["Ticker"] = trsy.Ticker;
                toInsert["Tenor"] = tenors[i++];
                toInsert["Ask"] = trsy.Ask;
                toInsert["Mid"] = trsy.Mid;
                toInsert["Bid"] = trsy.Bid;
                toInsert["Coupon"] = trsy.Coupon; 
                toInsert["BT"] = trsy.BT;
                toInsert["MaturityDate"] = trsy.MaturityDate;
                outputData.Rows.Add(toInsert);
            }
        }
        public class TreasuryDetail
        {
            public DateTime ValDate { get; set; }
            public DateTime MaturityDate { get; set; }
            public string Ticker { get; set; }
            public decimal Coupon { get; set; }
            public char BT { get; set; }
            public decimal Ask { get; set; }
            public decimal Mid { get; set; }
            public decimal Bid { get; set; }

        }
    }
}
