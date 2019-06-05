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

namespace DataLoaderOptions.Loaders
{
    class DataLoaderJP : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderJP()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "CSJSTMT";
        public int HeaderRow => 6;
        public string Counterparty => "JPMORGAN CHASE BANK, N.A.";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*" + FileNameSubString  + "* ", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                var ext = Path.GetExtension(file);
                if (ext.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase) || ext.Equals(".xls", StringComparison.InvariantCultureIgnoreCase))
                {
                    DataTable outputData = CreateDataTable();
                    DataReader_Excel xl = new DataReader_Excel(file, HeaderRow, password: "6NbO4m");
                    xl.SetHeaderRow(3, "Product Type:  EQ OPT");
                    xl.HeaderRow += 1;
                    xl.HeaderLength = 2;
                    xl.SetDataTableFormat(outputData);

                    var dateString = (string)xl.GetCell(8, 19);
                    string company = (string)xl.GetCell(2, 3);
                    var valDate = Extensions.Extensions.GetFirstDateFromString(dateString, @"\d{2}-[a-zA-Z]{3}-\d{4}$$", "dd-MMM-yyyy") ?? throw new Exception();

                    //xl.SetDateTimeFormat(@"Trade Date", @"d-MMM-yy");
                    //xl.SetDateTimeFormat(@"Mature Date", @"d-MMM-yy");
                    string fileName = OutputPath + "JP " + valDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);


                    outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                    CorrectDataTable(outputData, valDate, company);
                    lock (toLock)
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            DeleteDuplicates(outputData, con);
                            if (outputData.Rows.Count > 0)
                            {
                                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                                {
                                    //Set the database table name
                                    sqlBulkCopy.DestinationTableName = SqlTableName;
                                    MapTable(sqlBulkCopy);
                                    sqlBulkCopy.WriteToServer(outputData);
                                }
                                bool fileExists = File.Exists(fileName);
                                int i = 1;
                                while (fileExists)
                                {
                                    fileName = Path.GetDirectoryName(fileName) + Path.GetFileNameWithoutExtension(fileName) + "_" + i + Path.GetExtension(fileName);
                                    fileExists = File.Exists(fileName);
                                }
                                File.Move(file, fileName);
                            }
                            else
                            {
                                File.Delete(file);
                            }
                        }
                    }
                }
                else
                {
                    File.Delete(file);
                }
            }
        }
        protected void CorrectDataTable(DataTable outputData, DateTime asOfDate, string company)
        {
            outputData.Columns.Add(@"valdate", typeof(DateTime));
            outputData.Columns.Add(@"counterparty", typeof(string));
            outputData.Columns.Add(@"company", typeof(string));
            outputData.Columns.Add(@"units", typeof(string));
            outputData.Columns.Add(@"Buy/Sell", typeof(string));
            
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["USD Mtm"] == DBNull.Value || row["Trade Date"] == DBNull.Value)
                {
                    row.Delete();
                }
                else 
                {
                    row["valdate"] = asOfDate;
                    row["counterparty"] = Counterparty;
                    row["company"] = company;
                    if (row["Strike Price"] != DBNull.Value && row.Field<decimal>("Strike Price") != 0)
                    {
                        row["units"] = row.Field<decimal>("Rec Notional") / row.Field<decimal>("Strike Price");
                    }
                    row["USD Mtm"] = row.Field<decimal>("USD Mtm") * -1;
                    
                    if (row.Field<decimal>("Rec Notional") >0)
                    {
                        row[@"Buy/Sell"] = "Buy";
                        
                    }
                    else
                    {
                        row[@"Buy/Sell"] = "Sell";
                    }
                }
            }
            outputData.AcceptChanges();
        }
        protected void DeleteDuplicates(DataTable outputData, SqlConnection con)
        {
            int count = 0;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                using (SqlCommand sqlCommand = new SqlCommand(@"select count(*) from " + SqlTableName + " where ValDate = @ValDate " +
                                                                    "and Counterparty = @Counterparty and IssuerId = @IssuerId and CompanyCode = @CompanyCode", con))
                {

                    sqlCommand.Parameters.Add("@ValDate", SqlDbType.DateTime);
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("valdate");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("counterparty");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Source Deal ID");
                    sqlCommand.Parameters.Add("@CompanyCode", SqlDbType.VarChar);
                    sqlCommand.Parameters["@CompanyCode"].Value = row.Field<string>("company");
                    count = (Int32)sqlCommand.ExecuteScalar();
                }
                if (count > 0)
                {
                    row.Delete();
                }
            }
            outputData.AcceptChanges();
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(@"Source Deal ID", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Mature Date", typeof(DateTime));
            tbl.Columns.Add(@"Underlying Asset", typeof(string));
            tbl.Columns.Add(@"Strike Price", typeof(decimal));
            tbl.Columns.Add(@"Rec Notional", typeof(decimal));
            tbl.Columns.Add(@"USD Mtm", typeof(decimal));
            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("valdate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("company", "CompanyCode");
            sqlBulkCopy.ColumnMappings.Add("Source Deal ID", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Mature Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike Price", "Strike");
            sqlBulkCopy.ColumnMappings.Add("units", "Units");
            sqlBulkCopy.ColumnMappings.Add("Rec Notional", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("USD Mtm", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlying Asset", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Underlying Asset", "OptionType");
            
        }
    }
}