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
    class DataLoaderGS : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderGS()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "Trade_Detail_GSIL";
        public int HeaderRow => 10;
        public string Counterparty => "GOLDMAN SACHS INTERNATIONAL";
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
                    xl.SetDateTimeFormat(@"Trade Date", @"dd-MMM-yyyy");
                    xl.SetDateTimeFormat(@"Maturity Date", @"dd-MMM-yyyy");

                    DateTime? date = Extensions.Extensions.GetFirstDateFromString(file, @"\B\d{2}_\w{3}_\d{4}", "dd_MMM_yyyy");
                    if (date != null)
                    {
                        DateTime asOfDate = (DateTime)date;
                        outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                        CorrectDataTable(outputData);
                        string company = (string)xl.GetCell(2, 2);
                        string fileName = OutputPath + "GS " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
                        UpdateAsOfDate(outputData, asOfDate, company);
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
                if (row.Field<string>("GS Entity") == null || row.Field<string>("Trade Id") == null)
                {
                    row.Delete();
                }
                else 
                {
                    row["GS Entity"] = Counterparty;
                    row["NPV (USD)"] = row.Field<decimal>("NPV (USD)") * -1;
                    var buySell = row.Field<string>(@"Buy/Sell");
                    if (buySell.Equals("B",StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/Sell"] = "Sell";
                        row["Notional(2)"] = (object)(row.Field<decimal?>("Notional(2)") * -1) ?? DBNull.Value;
                        row["Notional(1)"] = row.Field<decimal>("Notional(1)") * -1;
                    }
                    else if(buySell.Equals("S", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy/Sell"] = "Buy";
                    }
                }
            }
            outputData.AcceptChanges();
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string company)
        {
            outputData.Columns.Add("CompanyAtRisk", typeof(string));
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["ValDate"] = asOfDate;
                row["CompanyAtRisk"] = company;
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
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("GS Entity");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Trade ID");
                    sqlCommand.Parameters.Add("@CompanyCode", SqlDbType.VarChar);
                    sqlCommand.Parameters["@CompanyCode"].Value = row.Field<string>("CompanyAtRisk");
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
            tbl.Columns.Add("Trade Id", typeof(string));
            tbl.Columns.Add("GS Entity", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date", typeof(DateTime));
            tbl.Columns.Add("Strike Price", typeof(decimal));
            tbl.Columns.Add("Notional(2)", typeof(decimal));
            tbl.Columns.Add("Notional(1)", typeof(decimal));
            tbl.Columns.Add(@"Buy/Sell", typeof(string));
            tbl.Columns.Add("NPV (USD)", typeof(decimal));
            tbl.Columns.Add("Underlier", typeof(string));
            tbl.Columns.Add("Transaction Type", typeof(string));


            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Trade Id", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("GS Entity", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike Price", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Notional(2)", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional(1)", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("NPV (USD)", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlier", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Transaction Type", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("CompanyAtRisk", "CompanyCode");
        }
    }
}