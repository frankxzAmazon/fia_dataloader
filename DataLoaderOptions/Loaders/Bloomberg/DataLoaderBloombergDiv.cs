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

namespace DataLoaderOptions
{
    class DataLoaderBloombergDiv : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        private static Dictionary<string, double> tenors = new Dictionary<string, double>()
        {
            ["1W"] = 0.0194444444444444,
            ["1M"] = 0.0833333333333333,
            ["2M"] = 0.166666666666667,
            ["3M"] = 0.25,
            ["6M"] = 0.5,
            ["9M"] = 0.75,
            ["1Y"] = 1,
            ["18M"] = 1.5,
            ["2Y"] = 2,
            ["3Y"] = 3,
            ["4Y"] = 4,
            ["5Y"] = 5,
            ["7Y"] = 7,
            ["10Y"] = 10
        };
        public DataLoaderBloombergDiv()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\";
        }

        public override string SqlTableName => "dbo.tblDivData";
        public async Task LoadData(int waitTimeInMinutes)
        {
            //Task t1 = Task.Delay(waitTimeInMinutes * 60 * 1000);
            Task t2 = Task.Run(() => LoadToSql());
            await Task.WhenAll(t2);
        }
        public override void LoadToSql()
        {
            lock (toLock)
            {
                string[] files = Directory.GetFiles(inputFolder.ToString(), "*Div*", SearchOption.AllDirectories);
                foreach (string file in files)
                {
                    DataTable outputData = CreateDataTable();
                    FillDataTable(outputData, GetConnString(file, true));
                    DateTime asOfDate = (DateTime)outputData.Rows[0]["FutureDate"];
                    // string fileName = OutputPath + asOfDate.ToString("yyyyMMdd") + "_" + Path.GetFileName(file);
                    string fileName = OutputPath  + Path.GetFileName(file);
                    //int changeLogId = base.CreateChangeLog(fileName, Path.GetFileNameWithoutExtension(file), "VolSurface", DateTime.Now);
                    CorrectDataTable(outputData, fileName);

                    string sqlString = ConfigurationManager.ConnectionStrings["MarketData"].ConnectionString;
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        var tickerDict = new Dictionary<(string ticker, DateTime valdate), bool>();
                        foreach (DataRow row in outputData.Rows)
                        {
                            if (row["Valdate"] == DBNull.Value || row["ticker"] == DBNull.Value)
                            {
                                row.Delete();
                            }
                            else
                            {
                                string ticker = row.Field<string>("Ticker");
                                var valDate = row.Field<DateTime>("ValDate");
                                if (!tickerDict.TryGetValue((ticker, valDate), out var toDelete))
                                {
                                    SqlCommand checkRowExists = new SqlCommand("SELECT COUNT(*) FROM " + SqlTableName + " WHERE [ticker] = @ticker " +
                                    "and [ValDate] = @valdate ", con);
                                    checkRowExists.Parameters.AddWithValue("@ticker", ticker);
                                    checkRowExists.Parameters.AddWithValue("@valdate", valDate);
                                    int count = (int)checkRowExists.ExecuteScalar();
                                    toDelete = count > 0;
                                    tickerDict[(ticker, valDate)] = toDelete;
                                }
                                if (toDelete)
                                {
                                    row.Delete();
                                }
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
                    if (File.Exists(fileName))
                    {
                        File.Delete(fileName);
                    }
                    File.Move(file, fileName);
                    if (ToLoad)
                    {
                        try
                        {
                            sqlString = ConfigurationManager.ConnectionStrings["Dev"].ConnectionString;
                            using (SqlConnection con = new SqlConnection(sqlString))
                            {
                                con.Open();
                                using (SqlCommand cmd = new SqlCommand("dbo.UpdateMarketData", con))
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
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("Ticker", typeof(string));
            tbl.Columns.Add("Tenor", typeof(string));
            tbl.Columns.Add("FutureDate", typeof(DateTime));
            tbl.Columns.Add("FutureValue", typeof(double));
            tbl.Columns.Add("DividendAmount", typeof(decimal));
            tbl.Columns.Add("DiscountFactor", typeof(double));

            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string fileName)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("UserId", typeof(string));
            outputData.Columns.Add("DividendType", typeof(string));
            outputData.Columns.Add("Valdate", typeof(DateTime));
            var dt = DateTime.UtcNow;
            var user = Environment.UserName;
            var priorRow = outputData.AsEnumerable()
                        .Where(x => x.Field<string>("Tenor") == "SPOT");

            foreach (DataRow row in outputData.Rows)
            {
                if (row.Field<string>("Tenor") != "SPOT")
                {
                    row["LoadDate"] = dt;
                    row["UserId"] = user;
                    row["DividendType"] = "Continuous";
                    row["Valdate"] = priorRow.FirstOrDefault(x => x.Field<string>("Ticker").Equals(row.Field<string>("Ticker"))).Field<DateTime>("FutureDate");
                    var tenor = tenors[(string)row["Tenor"]];
                    var discFactor = row.Field<double>("DiscountFactor");
                    //var priorRow = outputData.AsEnumerable()
                    //    .Where(x => x.Field<DateTime>("FutureDate") < row.Field<DateTime>("FutureDate"))
                    //    .OrderByDescending(x => x.Field<DateTime>("FutureDate"))
                    //    .FirstOrDefault();
                    var priorValue = priorRow.FirstOrDefault(x => x.Field<string>("Ticker").Equals(row.Field<string>("Ticker"))).Field<double>("FutureValue");
                    var value = row.Field<double>("FutureValue");
                    var impliedDiv = Math.Log(priorValue / (discFactor * value)) / tenor;
                    row["DividendAmount"] = (decimal)impliedDiv;
                }
            }
            outputData.AcceptChanges();
            foreach (DataRow row in outputData.Rows)
            {
                if ((string)row["Tenor"] == "SPOT")
                {
                    row.Delete();
                }
            }
            outputData.AcceptChanges();
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            //sqlBulkCopy.ColumnMappings.Add("idChangeLog", "idChangeLog");
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserId");
            sqlBulkCopy.ColumnMappings.Add("Ticker", "Ticker");
            sqlBulkCopy.ColumnMappings.Add("Valdate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Tenor", "Tenor");
            sqlBulkCopy.ColumnMappings.Add("DividendType", "DividendType");
            sqlBulkCopy.ColumnMappings.Add("DividendAmount", "Val");
        }
        protected void FillDataTable(DataTable outputData, string conString)
        {
            DateTime? asOfDate = null;
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
                                DataRow toInsert = outputData.NewRow();
                                int i = 0;
                                foreach (DataColumn col in outputData.Columns)
                                {
                                    if (col.ColumnName.Equals("Ticker", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        toInsert[col.ColumnName] = row[i++].Replace(" Index", "").TrimEnd(' ');
                                    }
                                    else if (col.ColumnName.Equals("FutureDate", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        toInsert[col.ColumnName] = DateTime.ParseExact(row[i++].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                                    }
                                    else if (row[i] != "")
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
                        else if (row[0].StartsWith("RUNDATE=", StringComparison.InvariantCultureIgnoreCase))
                        {
                            asOfDate = DateTime.ParseExact(row[0].Substring(8, row[0].Length - 8), "yyyyMMdd", CultureInfo.InvariantCulture);
                        }
                    }
                }
            }

        }
    }
}
