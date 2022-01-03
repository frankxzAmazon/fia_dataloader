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
    class DataLoaderBloombergIndex : DataLoader
    {
        Dictionary<string, string> index = new Dictionary<string, string>()
        {
            ["DBGLS2U5 Index"] = "DBGLS2U5",
            ["DBGLS2UP Index"] = "DBGLS2UP",
            ["DBGLS3U5 Index"] = "DBGLS3U5",
            ["DBMUAU55 Index"] = "DBMUAU55",
            ["DJI Index"] = "DJI",
            ["GSDYNMO5 Index"] = "GSDYNMO5",
            ["ILBAX Index"] = "ILBAX",
            ["MID Index"] = "MID",
            ["MOODCAVG Index"] = "MOODCAVG",
            ["MSUMSDSI Index"] = "MSUMSDSI",
            ["MSUSMSGO Index"] = "MSUSMSGO",
            ["NDX Index"] = "NDX",
            ["RTY Index"] = "RTY",
            ["SPMARC5P Index"] = "SPMARC5P",
            ["SPX Index"] = "SPX",
            ["SX5E Index"] = "SX5E",
            ["TPX Index"]= "TPX",
            ["HSI Index"]="HSI",
            ["UKX Index"]= "UKX",
            ["LBUSTRUU Index"]= "LBUSTRUU",
            ["BXIIF50E Index"]= "BXIIF50E",
            ["SPXSRT5E Index"]= "SPXSRT5E",
            ["SPECFR6P Index"]= "SPECFR6P",
            ["SPXT5UE Index"]= "SPXT5UE",
            ["SPLV5UT Index"]= "SPLV5UT",
            ["ASD1 Index"]= "ASD1",
            ["CMRBEY5E Index"]= "CMRBEY5E",
            ["BXFTCS5E Index"]= "BXFTCS5E",
            ["ESH0 Index"]="ESH0",
            ["ESH1 Index"] = "ESH1",
            ["ESH2 Index"] = "ESH2",
            ["ESH3 Index"] = "ESH3",
            ["ESH4 Index"] = "ESH4",
            ["ESH5 Index"] = "ESH5",
            ["ESH6 Index"] = "ESH6",
            ["ESH7 Index"] = "ESH7",
            ["ESH8 Index"] = "ESH8",
            ["ESH9 Index"] = "ESH9",
            ["ESU0 Index"] = "ESU0",
            ["ESU1 Index"] = "ESU1",
            ["ESU2 Index"] = "ESU2",
            ["ESU3 Index"] = "ESU3",
            ["ESU4 Index"] = "ESU4",
            ["ESU5 Index"] = "ESU5",
            ["ESU6 Index"] = "ESU6",
            ["ESU7 Index"] = "ESU7",
            ["ESU8 Index"] = "ESU8",
            ["ESU9 Index"] = "ESU9",
            ["ESM0 Index"] = "ESM0",
            ["ESM1 Index"] = "ESM1",
            ["ESM2 Index"] = "ESM2",
            ["ESM3 Index"] = "ESM3",
            ["ESM4 Index"] = "ESM4",
            ["ESM5 Index"] = "ESM5",
            ["ESM6 Index"] = "ESM6",
            ["ESM7 Index"] = "ESM7",
            ["ESM8 Index"] = "ESM8",
            ["ESM9 Index"] = "ESM9",
            ["ESZ0 Index"] = "ESZ0",
            ["ESZ1 Index"] = "ESZ1",
            ["ESZ2 Index"] = "ESZ2",
            ["ESZ3 Index"] = "ESZ3",
            ["ESZ4 Index"] = "ESZ4",
            ["ESZ5 Index"] = "ESZ5",
            ["ESZ6 Index"] = "ESZ6",
            ["ESZ7 Index"] = "ESZ7",
            ["ESZ8 Index"] = "ESZ8",
            ["ESZ9 Index"] = "ESZ9",
            ["ESA Index"]="ESA",
            ["RTYA Index"]="RTYA",
            ["MSFA Index"]="MFSA",
            ["NQA Index"]="NQA"


        };
        Dictionary<string, string> tickerDesc = new Dictionary<string, string>()
        {
            ["DBGLS2U5"] = "CROCI Sectors II 5.5% Volatility Control Index",
            ["DBGLS2UP"] = "CROCI Sectors II USD PR",
            ["DBGLS3U5"] = "CROCI Sectors III 5.5% Volatility Control Index",
            ["DBMUAU55"] = "MAA 5.5% Volatility Control Index",
            ["DJI"] = "Dow Jones",
            ["GSDYNMO5"] = "Goldman Sachs Dynamo Strategy Index",
            ["ILBAX"] = "Lehman Aggregate Bond Index",
            ["MID"] = "S&P Mid-Cap",
            ["MOODCAVG"] = "Moodys Corporate Average Bond Index",
            ["MSUMSDSI"] = "Morgan Stanley-MDSI",
            ["MSUSMSGO"] = "Morgan Stanley Global Allocation Index",
            ["NDX"] = "NASDAQ",
            ["RTY"] = "Russell 2000",
            ["SPMARC5P"] = "S&P Marc 5",
            ["SPX"] = "S&P 500",
            ["SX5E"] = "EURO STOXX",
            ["TPX"]= "Tokyo Price Index (TOPIX)",
            ["HSI"]= "Hong Kong Hang Seng",
            ["UKX"]= "FTSE 100",
            ["LBUSTRUU"]= "Barclays US Aggregate Bond Index",
            ["BXIIF50E"]="Barclasy Focus50 Index",
            ["SPXSRT5E"]="S &P 500 Sector Rotator Daily RC2 5% Index ER",
            ["SPECFR6P"]= "S&P Economic Cycle Factor Rotator Index",
            ["SPXT5UE"]= "S&P 500 Low Volatility Daily Risk Control 5% Index ER",
            ["SPLV5UT"]= "S&P 500 Low Volatility Daily Risk Control 5% Index TR",
            ["ASD1"] = "Generic 1st ASD future",
            ["CMRBEY5E"]="RBA select equity yield CIBC 5% Index",
            ["BXFTCS5E"]= "First Trust Capital Strength Barclays 5% Index",
            ["ESH0"] = "S&P mini future",
            ["ESH1"] = "S&P mini future",
            ["ESH2"] = "S&P mini future",
            ["ESH3"] = "S&P mini future",
            ["ESH4"] = "S&P mini future",
            ["ESH5"] = "S&P mini future",
            ["ESH6"] = "S&P mini future",
            ["ESH7"] = "S&P mini future",
            ["ESH8"] = "S&P mini future",
            ["ESH9"] = "S&P mini future",
            ["ESU0"] = "S&P mini future",
            ["ESU1"] = "S&P mini future",
            ["ESU2"] = "S&P mini future",
            ["ESU3"] = "S&P mini future",
            ["ESU4"] = "S&P mini future",
            ["ESU5"] = "S&P mini future",
            ["ESU6"] = "S&P mini future",
            ["ESU7"] = "S&P mini future",
            ["ESU8"] = "S&P mini future",
            ["ESU9"] = "S&P mini future",
            ["ESM0"] = "S&P mini future",
            ["ESM1"] = "S&P mini future",
            ["ESM2"] = "S&P mini future",
            ["ESM3"] = "S&P mini future",
            ["ESM4"] = "S&P mini future",
            ["ESM5"] = "S&P mini future",
            ["ESM6"] = "S&P mini future",
            ["ESM7"] = "S&P mini future",
            ["ESM8"] = "S&P mini future",
            ["ESM9"] = "S&P mini future",
            ["ESZ0"] = "S&P mini future",
            ["ESZ1"] = "S&P mini future",
            ["ESZ2"] = "S&P mini future",
            ["ESZ3"] = "S&P mini future",
            ["ESZ4"] = "S&P mini future",
            ["ESZ5"] = "S&P mini future",
            ["ESZ6"] = "S&P mini future",
            ["ESZ7"] = "S&P mini future",
            ["ESZ8"] = "S&P mini future",
            ["ESZ9"] = "S&P mini future",
            ["ESA"] = "E-mini S&P 500 Futures Active Contract",
            ["RTYA"] = "E-mini Russell 2000 Index Futures Active Contract",
            ["MSFA"] = "MSCI EAFE Index Futures Active Contract",
            ["NQA"] = "NASDAQ 100 E-mini Active Contract"


        };
        string inputFolder;
        static object toLock = new object();

        public DataLoaderBloombergIndex()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\";
        }

        public override string SqlTableName => "dbo.tblIndex";
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
                string[] files = Directory.GetFiles(inputFolder.ToString(), "*FIAHedgePrices*", SearchOption.AllDirectories);
                foreach (string file in files)
                {
                    DataTable outputData = CreateDataTable();
                    DateTime asOfDate = Extensions.Extensions.GetFirstDateFromString(Path.GetFileName(file), @"\d{8}", "yyyyMMdd") ?? throw new Exception();
                    FillDataTable(outputData, GetConnString(file, true), asOfDate);
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
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 23)}Morning.txt";
                        }
                        else if (hour <= 15)
                        {
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 23)}Noon.txt";
                        }
                        else
                        {
                            fileNewName = $"{Path.GetFileName(file).Substring(0, 23)}Afternoon.txt";
                        }
                         fileName = OutputPath + fileNewName;
                    }
                    //int changeLogId = base.CreateChangeLog(fileName, Path.GetFileNameWithoutExtension(file), "IndexSurface", DateTime.Now);
                    CorrectDataTable(outputData, fileName);
                    outputData.AcceptChanges();
                    string sqlString = ConfigurationManager.ConnectionStrings["MarketData"].ConnectionString;
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        foreach (DataRow row in outputData.Rows)
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
            tbl.Columns.Add("Random", typeof(string));
            tbl.Columns.Add("Random1", typeof(string));
            tbl.Columns.Add("PX_Last", typeof(decimal));
            tbl.Columns.Add("PX_CLOSE_DT", typeof(DateTime));
            tbl.Columns.Add("Last_Update", typeof(DateTime));



            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string fileName)
        {
            var dtime = DateTime.UtcNow;
            outputData.Columns.Add("Desc", typeof(string));
            outputData.Columns.Add("Source", typeof(string));
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("TimeStamp", typeof(DateTime));
            outputData.Columns.Add("UserId", typeof(string));
            outputData.Columns.Add("Valdate", typeof(DateTime));
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["Desc"] = tickerDesc[(string)row["Ticker"]];
                row["Source"] = "Bloomberg";
                row["LoadDate"] = dtime;
                row["TimeStamp"] = dtime;
                row["UserId"] = Environment.UserName;
                if (row["PX_CLOSE_DT"] != DBNull.Value)
                {
                    row["Valdate"] = row["PX_CLOSE_DT"];
                }
                else
                {
                    row["Valdate"] = row["Last_Update"];
                }
                if (row["PX_Last"] == DBNull.Value)
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
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("TimeStamp", "TimeStamp");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserId");
            sqlBulkCopy.ColumnMappings.Add("Desc", "Desc");
            sqlBulkCopy.ColumnMappings.Add("Valdate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Ticker", "Ticker");
            sqlBulkCopy.ColumnMappings.Add("PX_Last", "Val");
        }
        protected void FillDataTable(DataTable outputData, string conString, DateTime asOfDate)
        {
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
                                    if (row[i].Trim(' ') == "" || row[i] == "N.A.")
                                    {
                                        i++;
                                    }
                                    else if (col.ColumnName.Equals("Ticker", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        if (index.ContainsKey(row[i]))
                                            toInsert[col.ColumnName] = index[row[i++]];
                                        else
                                        {
                                            toInsert[col.ColumnName] = "";
                                            break;
                                        }
                                    }
                                    else if (col.ColumnName.Equals("Last_Update", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        if (!DateTime.TryParseExact(row[i++], "MM/dd/yyyy", CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime date))
                                        {
                                            date = asOfDate;
                                        }
                                        if (date == DateTime.MinValue)
                                        {
                                            date = asOfDate;
                                        }
                                        toInsert[col.ColumnName] = date;
                                    }
                                    else if (row[i] != "" && row[i] != "N.A.")
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
                                if ((string)toInsert["Ticker"] != "")
                                    outputData.Rows.Add(toInsert);
                            }
                        }
                    }
                }
            }

        }
    }
}
