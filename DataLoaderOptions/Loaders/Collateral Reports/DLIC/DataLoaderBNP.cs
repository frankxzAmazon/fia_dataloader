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
    class DataLoaderBNP : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderBNP()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "Exposure Statement - BNP PARIBAS";
        public int HeaderRow => 8;
        public string Counterparty => "BNP PARIBAS";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*" + FileNameSubString  + "* ", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                var ext = Path.GetExtension(file);
                if (ext.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase) || ext.Equals(".xls", StringComparison.InvariantCultureIgnoreCase))
                {
                    DataTable outputData = CreateDataTable();
                    DataReader_Excel xl = new DataReader_Excel(file, HeaderRow);
                    xl.SetDataTableFormat(outputData);
                    xl.SetDateTimeFormat(@"Exposure Date", @"dd/MM/yyyy");
                    xl.SetDateTimeFormat(@"Trade Date", @"dd/MM/yyyy");
                    xl.SetDateTimeFormat(@"Maturity Date", @"dd/MM/yyyy");
                    outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                    CorrectDataTable(outputData);

                    DateTime asOfDate = outputData.AsEnumerable().FirstOrDefault().Field<DateTime>("Exposure Date");
                    string company = outputData.AsEnumerable().FirstOrDefault().Field<string>("Source Counterparty");
                    string fileName = OutputPath + "BNP " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
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
        protected void CorrectDataTable(DataTable outputData)
        {
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row.Field<string>("Source Principal") == null || row.Field<string>("Trade Ref") == null)
                {
                    row.Delete();
                }
                else 
                {
                    row["Source Principal"] = Counterparty;
                    row["Exposure Amount (Agmt Ccy)"] = row.Field<decimal>("Exposure Amount (Agmt Ccy)") * -1;
                    var buySell = row.Field<string>(@"Buy/Sell");
                    if (buySell.Equals("Buy",StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/Sell"] = "Sell";
                        row["Quantity"] = (object)(row.Field<decimal?>("Quantity") * -1) ?? DBNull.Value;
                        row["Notional 1"] = row.Field<decimal>("Notional 1") * -1;
                    }
                    else if(buySell.Equals("Sell", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/Sell"] = "Buy";
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
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("Exposure Date");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("Source Principal");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Trade Ref");
                    sqlCommand.Parameters.Add("@CompanyCode", SqlDbType.VarChar);
                    sqlCommand.Parameters["@CompanyCode"].Value = row.Field<string>("Source Counterparty");
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
            tbl.Columns.Add("Exposure Date", typeof(DateTime));
            tbl.Columns.Add("Trade Ref", typeof(string));
            tbl.Columns.Add("Source Principal", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date", typeof(DateTime));
            tbl.Columns.Add("Strike Price", typeof(decimal));
            tbl.Columns.Add("Quantity", typeof(decimal));
            tbl.Columns.Add("Notional 1", typeof(decimal));
            tbl.Columns.Add(@"Buy/Sell", typeof(string));
            tbl.Columns.Add("Exposure Amount (Agmt Ccy)", typeof(decimal));
            tbl.Columns.Add("Underlying", typeof(string));
            tbl.Columns.Add("Option Type", typeof(string));
            tbl.Columns.Add("Source Counterparty", typeof(string));

            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("Exposure Date", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Trade Ref", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Source Principal", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike Price", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Quantity", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional 1", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("Exposure Amount (Agmt Ccy)", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlying", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Option Type", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("Source Counterparty", "CompanyCode");
        }
    }
}