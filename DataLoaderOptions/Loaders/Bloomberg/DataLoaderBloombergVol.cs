using CsvHelper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataLoaderOptions
{
    class DataLoaderBloombergVol : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderBloombergVol()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Bloomberg\\";
        }

        public override string SqlTableName => "dbo.tblVolData";
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
                string[] files = Directory.GetFiles(inputFolder.ToString(), "*BVOL*", SearchOption.AllDirectories);
                foreach (string file in files)
                {
                    // Only load the one that was created after 5PM
                    bool loadVolFile = false;
                    if (Path.GetFileName(file).Contains("Afternoon"))
                    {
                         loadVolFile = true;
                    }
                    else
                    {
                        int hour = Int32.Parse(file.Substring(file.IndexOf("_") + 1, 2));
                        if (hour >= 17)
                            {
                            loadVolFile = true;
                            }
                    }
                    if (loadVolFile==true)
                    {
                        DataTable outputData = CreateDataTable();

                        DateTime asOfDate = Extensions.Extensions.GetFirstDateFromString(file, @"\d{8}", "yyyyMMdd") ?? throw new Exception();
                        FillDataTable(outputData, GetConnString(file, true), asOfDate);
                        string fileName = $"{OutputPath}{asOfDate.ToString("yyyyMMdd")}BVOL_Afternoon.txt";
                        if (!File.Exists(fileName))
                        {
                            //int changeLogId = base.CreateChangeLog(fileName, Path.GetFileNameWithoutExtension(file), "VolSurface", DateTime.Now);
                            CorrectDataTable(outputData, fileName);
                            outputData.AcceptChanges();


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
                                            //SqlCommand checkRowExists = new SqlCommand("SELECT COUNT(*) FROM " + SqlTableName + " WHERE [Index] = @ticker " +
                                            //"and [ValDate] = @valdate " +
                                            //"and Source = 'bloomberg'", con);
                                            SqlCommand deleteMarketVolTable = new SqlCommand("delete FROM " + SqlTableName + " WHERE [Index] = @ticker " +
                                            "and [ValDate] = @valdate " +
                                            "and Source = 'bloomberg'", con);
                                            deleteMarketVolTable.Parameters.AddWithValue("@ticker", ticker);
                                            deleteMarketVolTable.Parameters.AddWithValue("@valdate", valDate);
                                            deleteMarketVolTable.ExecuteNonQuery();

                                            SqlCommand deleteOptVolTable = new SqlCommand("Update  OptionValuationDevelopment.Market.tblVOlatility " +
                                             "set ValidTo=GETUTCDATE() " +
                                             "WHERE [VolatilitySurfaceName] in (@ticker, iif(@ticker='spx', @tickerAnalytic,''))  " +
                                             "and [ValDate] = @valdate " +
                                             "and ValidTo='9999-12-31'  ", con);
                                            deleteOptVolTable.Parameters.AddWithValue("@ticker", ticker);
                                            deleteOptVolTable.Parameters.AddWithValue("@tickerAnalytic", ticker + " Analytical");
                                            deleteOptVolTable.Parameters.AddWithValue("@valdate", valDate);
                                            deleteOptVolTable.ExecuteNonQuery();

                                            tickerDict[(ticker, valDate)] = true;
                                        }
                                        //if (toDelete)
                                        //{
                                        //    row.Delete();
                                        //}
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
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("ValDate", typeof(DateTime));
            tbl.Columns.Add("Ticker", typeof(string));
            tbl.Columns.Add("Tenor", typeof(string));
            tbl.Columns.Add("MaturityDate", typeof(string));
            tbl.Columns.Add("StrikeToRefRatio", typeof(decimal));
            tbl.Columns.Add("RefValue", typeof(decimal));
            tbl.Columns.Add("VolatilityRate", typeof(decimal));
            tbl.Columns.Add("Source", typeof(string));
            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string fileName)
        {
            foreach (DataRow row in outputData.Rows)
            {
                row["VolatilityRate"] = (decimal)row["VolatilityRate"] / 100;
                row["Source"] = "Bloomberg";
            }
        }
        protected void CorrectDataTable(DataTable outputData, int idChangeLog)
        {
            outputData.Columns.Add("idChangeLog", typeof(int));
            foreach (DataRow row in outputData.Rows)
            {
                row["idChangeLog"] = idChangeLog;
                row["VolatilityRate"] = (decimal)row["VolatilityRate"] / 100;
            }
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            //sqlBulkCopy.ColumnMappings.Add("idChangeLog", "idChangeLog");
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("Ticker", "Index");
            sqlBulkCopy.ColumnMappings.Add("RefValue", "SpotRef");
            sqlBulkCopy.ColumnMappings.Add("Tenor", "InputMaturity");
            sqlBulkCopy.ColumnMappings.Add("StrikeToRefRatio", "StrikeToRefRatio");
            sqlBulkCopy.ColumnMappings.Add("VolatilityRate", "Volatility");
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
                                    if (col.ColumnName.Equals("ValDate", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        toInsert[col.ColumnName] = asOfDate;
                                    }
                                    else if (col.ColumnName.Equals("Ticker", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        toInsert[col.ColumnName] = row[i++].Replace(" Index", "").TrimEnd(' ');
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
                    }
                }
            }

        }
    }
}
