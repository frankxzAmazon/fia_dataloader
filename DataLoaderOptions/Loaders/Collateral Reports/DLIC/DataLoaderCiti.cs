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
    class DataLoaderCiti : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderCiti()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "Portfolio";
        public int HeaderRow => 9;
        public string Counterparty => "CITIBANK, N.A.";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*" + FileNameSubString + "* ", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                var ext = Path.GetExtension(file);
                if (ext.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase) || ext.Equals(".xls", StringComparison.InvariantCultureIgnoreCase))
                {
                    DataTable outputData = CreateDataTable();
                    DataReader_Excel xl = new DataReader_Excel(file, HeaderRow);
                    xl.SetDataTableFormat(outputData);
                    DateTime asOfDate = (DateTime)xl.GetCell(5, 2);
                    string company = (string)xl.GetCell(6, 2);
                    string fileName = OutputPath + "CITI " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
                    lock (toLock)
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                            CorrectDataTable(outputData);
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
        protected void CorrectDataTable(DataTable outputData)
        {
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row.Field<string>("Trade ID") == null || row.Field<decimal?>("Market Value") == null)
                {
                    row.Delete();
                }
                else
                {
                    row["Counterparty"] = Counterparty;
                    row["Market Value"] = row.Field<decimal>("Market Value") * -1;
                    if(row.Field<decimal?>("Strike Rate") != 0)
                        row["Units"] = (object)(row.Field<decimal?>("Notional") / row.Field<decimal?>("Strike Rate")) ?? DBNull.Value;
                    else
                        row["Units"] = DBNull.Value;
                    var buySell = row.Field<string>(@"BuySellIndicator");
                    if(string.IsNullOrWhiteSpace(buySell))
                    {

                    }
                    else if (buySell.Equals("Buy", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"BuySellIndicator"] = "Sell";
                        row["Notional"] = row.Field<decimal>("Notional") * -1;
                        row["Units"] = row.Field<decimal>("Units") * -1;
                    }
                    else if (buySell.Equals("Sell", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"BuySellIndicator"] = "Buy";
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
                using (SqlCommand sqlCommand = new SqlCommand("select count(*) from " + SqlTableName + " where ValDate = @ValDate and Counterparty = @Counterparty and IssuerId = @IssuerId", con))
                {

                    sqlCommand.Parameters.Add("@ValDate", SqlDbType.DateTime);
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("Market Date");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("Counterparty");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Trade ID");

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
            tbl.Columns.Add("Market Date", typeof(DateTime));
            tbl.Columns.Add("Trade ID", typeof(string));
            tbl.Columns.Add("Counterparty", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Term Date", typeof(DateTime));
            tbl.Columns.Add("Strike Rate", typeof(decimal));
            tbl.Columns.Add("Units", typeof(decimal));
            tbl.Columns.Add("Notional", typeof(decimal));
            tbl.Columns.Add(@"BuySellIndicator", typeof(string));
            tbl.Columns.Add("Market Value", typeof(decimal));
            tbl.Columns.Add("Description", typeof(string));
            tbl.Columns.Add("TypeOfOTCOption", typeof(string));
            tbl.Columns.Add("Counter Party Name", typeof(string));

            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("Market Date", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Trade ID", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Term Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike Rate", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Units", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"BuySellIndicator", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("Market Value", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Description", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("TypeOfOTCOption", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("Counter Party Name", "CompanyCode");
        }
    }
}