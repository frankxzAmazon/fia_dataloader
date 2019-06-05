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
    class DataLoaderMS : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderMS()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "NETIEDOPTION";
        public int HeaderRow => 1;
        public string Counterparty => "MORGAN STANLEY & CO INTERNAT'L PLC";
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
                    xl.SetDateTimeFormat(@"TradeDate", @"dd MMM yyyy");

                    DateTime? date = Extensions.Extensions.GetFirstDateFromString(file, @"\B\d{8}|B", "yyyyMMdd");
                    if (date != null)
                    {
                        DateTime asOfDate = (DateTime)date;
                        outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                        CorrectDataTable(outputData);

                        string company = outputData.AsEnumerable().FirstOrDefault().Field<string>("CPAccount").Trim();
                        if (company.Equals("03385AHP4", StringComparison.InvariantCultureIgnoreCase))
                        {
                            company = "DLIC";
                        }
                        else if (company.Equals("03385VUF5", StringComparison.InvariantCultureIgnoreCase))
                        {
                            company = "GLAC";
                        }
                        UpdateAsOfDate(outputData, asOfDate, company);

                        string fileName = OutputPath + "MS " + asOfDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);

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
            outputData.Columns.Add("Counterparty", typeof(string));
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row.Field<string>("CPAccount") == null || row.Field<string>("Cusip") == null)
                {
                    row.Delete();
                }
                else 
                {
                    row["Counterparty"] = Counterparty;
                    row["TotalExposureAgrCcy"] = row.Field<decimal>("TotalExposureAgrCcy") * -1;
                    var buySell = row.Field<string>(@"Buy_Sell");
                    if (buySell.Equals("B",StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy_Sell"] = "Buy";
                    }
                    else if(buySell.Equals("S", StringComparison.InvariantCultureIgnoreCase))
                    {
                        row[@"Buy_Sell"] = "Sell";
                        row["Position"] = (object)(row.Field<decimal?>("Position") * -1) ?? DBNull.Value;
                    }
                }
            }
            outputData.AcceptChanges();
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string company)
        {
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["ValDate"] = asOfDate;
                row["CPAccount"] = company;
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
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Cusip");
                    sqlCommand.Parameters.Add("@CompanyCode", SqlDbType.VarChar);
                    sqlCommand.Parameters["@CompanyCode"].Value = row.Field<string>("CPAccount");
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
            tbl.Columns.Add("Cusip", typeof(string));
            tbl.Columns.Add("CPAccount", typeof(string));
            tbl.Columns.Add(@"TradeDate", typeof(DateTime));
            tbl.Columns.Add(@"ExpDate", typeof(DateTime));
            tbl.Columns.Add("StrikeRisk", typeof(decimal));
            tbl.Columns.Add("Position", typeof(decimal));
            tbl.Columns.Add("Notional(1)", typeof(decimal));
            tbl.Columns.Add(@"Buy_Sell", typeof(string));
            tbl.Columns.Add("TotalExposureAgrCcy", typeof(decimal));
            tbl.Columns.Add("Underlier", typeof(string));
            tbl.Columns.Add("Put_Call", typeof(string));


            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("ValDate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("Cusip", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("TradeDate", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("ExpDate", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("StrikeRisk", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Position", "Units");
            sqlBulkCopy.ColumnMappings.Add("Notional(1)", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy_Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("TotalExposureAgrCcy", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlier", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Put_Call", "OptionType");
            sqlBulkCopy.ColumnMappings.Add("CPAccount", "CompanyCode");
        }
    }
}