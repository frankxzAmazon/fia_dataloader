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
    class DataLoaderCS : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderCS()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "CollateralCpty";
        public int HeaderRow => 6;
        public string Counterparty => "CREDIT SUISSE INTERNATIONAL";
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

                    var dateString = (string)xl.GetCell(1, 1);
                    string company = (string)xl.GetCell(2, 2);
                    var valDate = Extensions.Extensions.GetFirstDateFromString(dateString, @"\d{1,2} [a-zA-Z]{3} \d{4}$$", "d MMM yyyy") ?? throw new Exception();

                    //xl.SetDateTimeFormat(@"Trade Date", @"d-MMM-yy");
                    //xl.SetDateTimeFormat(@"Maturity Date", @"d-MMM-yy");
                    string fileName = OutputPath + "CS " + valDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
                    outputData = xl.GetFilledDataTable(OnError.UseNullValue);

                    xl.SetHeaderRow(1, "Equity Index Option");
                    xl.HeaderRow = xl.HeaderRow - 1;
                    outputData.Merge(xl.GetFilledDataTable(OnError.UseNullValue));
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
        protected void DeleteDuplicates(DataTable outputData, SqlConnection con)
        {
            int count = 0;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                using (SqlCommand sqlCommand = new SqlCommand(@"select count(*) from " + SqlTableName + " where ValDate = @ValDate "+
                                                                    "and Counterparty = @Counterparty and IssuerId = @IssuerId and CompanyCode = @CompanyCode", con))
                {

                    sqlCommand.Parameters.Add("@ValDate", SqlDbType.DateTime);
                    sqlCommand.Parameters["@ValDate"].Value = row.Field<DateTime>("valdate");
                    sqlCommand.Parameters.Add("@Counterparty", SqlDbType.VarChar);
                    sqlCommand.Parameters["@Counterparty"].Value = row.Field<string>("counterparty");
                    sqlCommand.Parameters.Add("@IssuerId", SqlDbType.VarChar);
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Trade ID");
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
        protected void CorrectDataTable(DataTable outputData, DateTime asOfDate, string company)
        {
            outputData.Columns.Add(@"valdate", typeof(DateTime));
            outputData.Columns.Add(@"counterparty", typeof(string));
            outputData.Columns.Add(@"company", typeof(string));

            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["Trade Date"] == DBNull.Value && row["Earliest Trade Date"] == DBNull.Value)
                {
                    row.Delete();
                }
                else 
                {
                    row["valdate"] = asOfDate;
                    row["counterparty"] = Counterparty;
                    row["company"] = company;
                    

                    if(row["Earliest Trade Date"] != DBNull.Value)
                    {
                        row["Trade Date"] = row["Earliest Trade Date"];
                        row["Notional1"] = 0;
                        row["Maturity Date"] = row["Swap Maturity Date"];
                        row["Rate"] = 0;
                        row["Quantity"] = -row.Field<decimal>("Quantity");
                    }
                    else if(row.Field<decimal>("Rate") != 0)
                    {
                        row["Quantity"] = -row.Field<decimal>("Notional1");
                        row["Notional1"] = row.Field<decimal>("Quantity") * row.Field<decimal>("Rate");
                    }
                    else
                    {
                        row["Quantity"] = 0;
                    }
                    row["PV (USD)"] = row.Field<decimal>("PV (USD)") * -1;
                    
                    
                    if (row.Field<decimal>("Notional1") >0 || row.Field<decimal>("Quantity") > 0)
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
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(@"Structure ID", typeof(string));
            tbl.Columns.Add(@"Trade ID", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Effective Date", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date", typeof(DateTime));
            tbl.Columns.Add(@"Description", typeof(string));
            tbl.Columns.Add(@"Buy/Sell", typeof(string));
            tbl.Columns.Add(@"Rate", typeof(decimal));
            tbl.Columns.Add(@"Notional1", typeof(decimal));
            
            tbl.Columns.Add(@"PV (USD)", typeof(decimal));
            tbl.Columns.Add(@"Earliest Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Swap Maturity Date", typeof(DateTime));
            tbl.Columns.Add(@"Quantity", typeof(decimal));
            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("valdate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("company", "CompanyCode");
            sqlBulkCopy.ColumnMappings.Add("Trade ID", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Rate", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Quantity", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional1", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("PV (USD)", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Description", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Description", "OptionType");
            
        }
    }
}