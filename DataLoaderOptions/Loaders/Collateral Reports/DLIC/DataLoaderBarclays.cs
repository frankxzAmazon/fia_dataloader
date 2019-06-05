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
    class DataLoaderBarclays : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderBarclays()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "CVGTradeValuation";
        public int HeaderRow => 10;
        public string Counterparty => "BARCLAYS BANK PLC";
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
                    xl.SetDateTimeFormat(@"Trade Date (dd/mm/yyyy)", @"dd/MM/yyyy");
                    xl.SetDateTimeFormat(@"Maturity Date (dd/mm/yyyy)", @"dd/MM/yyyy");

                    DateTime? date = Extensions.Extensions.GetFirstDateFromString(file, @"\b\d{2}\ \w{3} \d{4}\b", "dd MMM yyyy");
                    if (date != null)
                    {
                        DateTime asOfDate = (DateTime)date;
                        outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                        CorrectDataTable(outputData);
                        UpdateAsOfDate(outputData, asOfDate);
                        string company = outputData.AsEnumerable().FirstOrDefault().Field<string>("Counterparty");
                        string fileName = OutputPath + "BC " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
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
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row.Field<string>("Principal") == null || row.Field<string>("Trade Reference") == null)
                {
                    row.Delete();
                }
                else 
                {
                    row["Principal"] = Counterparty;
                    row["Notional1"] = row.Field<decimal>("Notional1") * -1;
                    row["Notional2"] = row.Field<decimal>("Notional2") * -1;
                    row["MTM (USD)"] = row.Field<decimal>("MTM (USD)") * -1;
                    var buySell = row.Field<string>(@"Buy/ Sell");
                    if (buySell.Equals("Buy",StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/ Sell"] = "Sell";
                    }
                    else if(buySell.Equals("Sell", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/ Sell"] = "Buy";
                    }
                    if ((row.Field<decimal>("Notional1") == row.Field<decimal>("Notional2")) && row.Field<decimal>("Strike Rate") != 0)
                    {
                        row["Notional1"] = row.Field<decimal>("Notional1") / row.Field<decimal>("Strike Rate");
                    }
                }
            }
            outputData.AcceptChanges();
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate)
        {
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["ValDate"] = asOfDate;
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
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("ValDate");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("Principal");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Trade Reference");
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
            tbl.Columns.Add("ValDate", typeof(DateTime));
            tbl.Columns.Add("Trade Reference", typeof(string));
            tbl.Columns.Add("Principal", typeof(string));
            tbl.Columns.Add(@"Trade Date (dd/mm/yyyy)", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date (dd/mm/yyyy)", typeof(DateTime));
            tbl.Columns.Add("Strike Rate", typeof(decimal));
            tbl.Columns.Add("Notional1", typeof(decimal));
            tbl.Columns.Add("Notional2", typeof(decimal));
            tbl.Columns.Add(@"Buy/ Sell", typeof(string));
            tbl.Columns.Add("MTM (USD)", typeof(decimal));
            tbl.Columns.Add("Index", typeof(string));
            tbl.Columns.Add("Product", typeof(string));
            tbl.Columns.Add("Counterparty", typeof(string));

            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Trade Reference", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Principal", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date (dd/mm/yyyy)", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date (dd/mm/yyyy)", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike Rate", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Notional1", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional2", "Notional");
            sqlBulkCopy.ColumnMappings.Add("Buy/ Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("MTM (USD)", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Index", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Product", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "CompanyCode");
        }
    }
}