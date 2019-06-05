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
    class DataLoaderDB : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderDB()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "ExposureStatement_";
        public int HeaderRow => 45;
        public string Counterparty => "DEUTSCHE BANK AG";
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

                    DateTime asOfDate = DateTime.Parse((string)xl.GetCell(12, 3));
                    
                    string company = (string)xl.GetCell(11,3);
                    string fileName = OutputPath + "DB " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
                    lock (toLock)
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                        outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                        CorrectDataTable(outputData);
                        UpdateAsOfDate(outputData, asOfDate, company);
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
                if (row.Field<string>("Trade ID") == null || row.Field<decimal?>("Market Value") == null)
                {
                    row.Delete();
                }
                else
                {
                    row["Counterparty"] = Counterparty;
                    row["Market Value"] = row.Field<decimal>("Market Value") * -1;
                    row["Notional1"] = row.Field<decimal>("Notional1") * -1;
                    if (row.Field<decimal?>("Strike") != 0)
                    {
                        row["Units"] = (object)(row.Field<decimal?>("Notional1") / row.Field<decimal?>("Strike")) ?? DBNull.Value;
                    }
                    var buySell = row.Field<string>(@"BuySell");
                    if (string.IsNullOrWhiteSpace(buySell))
                    {

                    }
                    else if (buySell.Equals("Buy", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"BuySell"] = "Sell";
                    }
                    else if (buySell.Equals("Sell", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"BuySell"] = "Buy";
                    }
                    if (row.Field<string>("Product Type").Equals("HybSwapIdx_MOMVOL",StringComparison.InvariantCultureIgnoreCase))
                    {
                        row["Underlier"] = "DBMUAU55";
                    }
                    else
                    {
                        row["Underlier"] = row.Field<string>("Underlier").Replace(".","");
                    }
                }
            }
            outputData.AcceptChanges();
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string company)
        {
            outputData.Columns.Add("ValDate", typeof(DateTime));
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
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("Counterparty");
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
            
            tbl.Columns.Add("Trade ID", typeof(string));
            tbl.Columns.Add("Counterparty", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date", typeof(DateTime));
            tbl.Columns.Add("Strike", typeof(decimal));
            tbl.Columns.Add("Units", typeof(decimal));
            tbl.Columns.Add("Notional1", typeof(decimal));
            tbl.Columns.Add(@"BuySell", typeof(string));
            tbl.Columns.Add("Market Value", typeof(decimal));
            tbl.Columns.Add("Underlier", typeof(string));
            tbl.Columns.Add("Product Type", typeof(string));


            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Trade ID", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Units", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional1", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"BuySell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("Market Value", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlier", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Product Type", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("CompanyAtRisk", "CompanyCode");
        }
    }
}