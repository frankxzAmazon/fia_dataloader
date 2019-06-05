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
    class DataLoaderWF : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderWF()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "TradesReport";
        public int HeaderRow => 1;
        public string Counterparty => "WELLS FARGO BANK, N.A.";
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

                    if (file.Contains("Daily") || file.Contains("WELLE"))
                    {
                        File.Delete(file);
                    }
                    else
                    {

                        outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                        CorrectDataTable(outputData);
                        DateTime asOfDate = outputData.AsEnumerable().FirstOrDefault().Field<DateTime>("effectiveDate");
                        string company = outputData.AsEnumerable().FirstOrDefault().Field<string>("Counterparty");
                        string fileName = OutputPath + "WF " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);


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
                }
                else
                {
                    File.Delete(file);
                }
        }
        }
        protected void CorrectDataTable(DataTable outputData)
        {
            outputData.Columns.Add("Units", typeof(decimal));
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row.Field<string>("tradeIdentifier2PartyA") == null || row.Field<decimal?>("valuationBaseCurrencyAmount") == null)
                {
                    row.Delete();
                }
                else
                {
                    row["Principal"] = Counterparty;
                    
                    if (row.Field<decimal?>("strikePrice") != 0)
                    {
                        row["Units"] = (object)(row.Field<decimal?>("exchangedNotional1amount") / row.Field<decimal?>("strikePrice")) ?? DBNull.Value;
                    }
                    var buySell = row.Field<string>(@"buySell");
                    if (string.IsNullOrWhiteSpace(buySell))
                    {

                    }
                    else if (buySell.Equals("Buy", StringComparison.InvariantCultureIgnoreCase))
                    {
                        
                        row[@"buySell"] = "Buy";
                    }
                    else if (buySell.Equals("Sell", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row["exchangedNotional1amount"] = row.Field<decimal>("exchangedNotional1amount") * -1;
                        row[@"buySell"] = "Sell";
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
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("effectiveDate");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("Principal");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("tradeIdentifier2PartyA");
                    sqlCommand.Parameters.Add("@CompanyCode", SqlDbType.VarChar);
                    sqlCommand.Parameters["@CompanyCode"].Value = row.Field<string>("Counterparty");
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
            tbl.Columns.Add(@"effectiveDate", typeof(DateTime));
            tbl.Columns.Add("tradeIdentifier2PartyA", typeof(string));
            tbl.Columns.Add("Principal", typeof(string));
            tbl.Columns.Add(@"tradeDate", typeof(DateTime));
            tbl.Columns.Add(@"maturityDate", typeof(DateTime));
            tbl.Columns.Add("strikePrice", typeof(decimal));
            tbl.Columns.Add("exchangedNotional1amount", typeof(decimal));
            tbl.Columns.Add(@"buySell", typeof(string));
            tbl.Columns.Add("valuationBaseCurrencyAmount", typeof(decimal));
            tbl.Columns.Add("underlying", typeof(string));
            tbl.Columns.Add("productType", typeof(string));
            tbl.Columns.Add("Counterparty", typeof(string));

            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("effectiveDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("tradeIdentifier2PartyA", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Principal", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("tradeDate", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("maturityDate", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("strikePrice", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Units", "Units");
            sqlBulkCopy.ColumnMappings.Add("exchangedNotional1amount", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"buySell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("valuationBaseCurrencyAmount", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("underlying", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("productType", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "CompanyCode");
        }
    }
}