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
    class DataLoaderBloombergRates : DataLoader
    {
        Dictionary<string, string> ignoreRates = new Dictionary<string, string>()
        {
            ["USSO4 BGN Curcny"]="",
            ["USSO1Z BGN Comdty"]="",
            ["USSWAP10 BGN Curncy"] = "USSWAP10",
            ["USSWAP12 BGN Curncy"] = "USSWAP12",
            ["USSWAP15 BGN Curncy"] = "USSWAP15",
            ["USSWAP20 BGN Curncy"] = "USSWAP20",
            ["USSWAP25 BGN Curncy"] = "USSWAP25",
            ["USSWAP30 BGN Curncy"] = "USSWAP30",
            ["USSWAP7 BGN Curncy"] = "USSWAP7",
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
        };
        string inputFolder;
        static object toLock = new object();
        public DataLoaderBloombergRates()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\";
        }

        public override string SqlTableName => "dbo.tblRates";
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
                    string fileName = OutputPath + Path.GetFileName(file);

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
                        if (File.Exists(fileName))
                        {
                            File.Delete(fileName);
                        }
                        File.Move(file, fileName);
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
            foreach (DataRow row in outputData.Rows)
            {
                row["Desc"] = row["Ticker"];
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
            }
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
                                        if (!ignoreRates.ContainsKey(row[i]))
                                            toInsert[col.ColumnName] = row[i++].Split(' ')[0];
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
                                            date = (DateTime)asOfDate;
                                        }
                                        if (date == DateTime.MinValue)
                                        {
                                            date = (DateTime)asOfDate;
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
