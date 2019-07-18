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
                    string fileName;
                    if (Path.GetFileName(file).Contains("Afternoon") || Path.GetFileName(file).Contains("Noon") || Path.GetFileName(file).Contains("Morning"))
                    {
                         fileName = OutputPath + Path.GetFileName(file);
                    }
                    else
                    {
                        int hour = Int32.Parse(file.Substring(file.IndexOf("_") + 1, 2));
                        string fileNewName;
                        if (hour <= 10)
                        {
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 19)}Morning.txt";
                        }
                        else if (hour <= 15)
                        {
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 19)}Noon.txt";
                        }
                        else
                        {
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 19)}Afternoon.txt";
                        }
                         fileName = OutputPath + fileNewName;
                    }
                   // string fileName = OutputPath  + Path.GetFileName(file);
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
            var firstRow = outputData.AsEnumerable()
                        .Where(x => x.Field<string>("Tenor") == "SPOT");

            for (int i = 0; i < outputData.Rows.Count;i++)
            {
//outputData.Rows[i]["Tenor"] != "SPOT"
                if (outputData.Rows[i].Field<string>("Tenor") != "SPOT")
                {
                    outputData.Rows[i]["LoadDate"] = dt;
                    outputData.Rows[i]["UserId"] = user;
                    outputData.Rows[i]["DividendType"] = "Continuous";
                    outputData.Rows[i]["Valdate"] = firstRow.FirstOrDefault(x => x.Field<string>("Ticker").Equals(outputData.Rows[i].Field<string>("Ticker"))).Field<DateTime>("FutureDate");
                    //var tenor = tenors[(string)outputData.Rows[i]["Tenor"]];
                    DateTime priorDate = outputData.Rows[i - 1].Field<DateTime>("FutureDate");
                    DateTime CurrDate = outputData.Rows[i].Field<DateTime>("FutureDate");
                    double tenor = Convert.ToDouble((CurrDate - priorDate).TotalDays.ToString())/360;
                    double discFactor = outputData.Rows[i].Field<double>("DiscountFactor");
                    double priorDiscFactor = outputData.Rows[i-1].Field<double>("DiscountFactor");


                    var priorValue = outputData.Rows[i - 1].Field<double>("FutureValue");
                    var value = outputData.Rows[i].Field<double>("FutureValue");
                    //var test = priorDiscFactor * priorValue;
                   // var test_2 = discFactor * value;
                    //decimal impliedDiv=Math.Log(test/test_2)
                    var impliedDiv = Math.Log(priorValue * priorDiscFactor / (value * discFactor)) / tenor;
                    outputData.Rows[i]["DividendAmount"] = (decimal)impliedDiv;

                    //if (outputData.Rows[i].Field<string>("Tenor") != "1W")
                    //{
                    //var priortenor = tenors[(string)outputData.Rows[i - 1]["Tenor"]];
                    //var impliedDiv = Math.Log(priorValue / (discFactor * value)) / tenor;
                    //outputData.Rows[i]["DividendAmount"] = (decimal)impliedDiv;
                    //}
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
            sqlBulkCopy.ColumnMappings.Add("FutureValue", "ForwardPrice");
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
