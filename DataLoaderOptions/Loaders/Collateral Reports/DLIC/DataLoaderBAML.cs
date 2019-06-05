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
    class DataLoaderBAML : DataLoader
    {
        string inputFolder;
        static object toLock = new object();

        public DataLoaderBAML()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\Collateral Reports\\";
        }

        public override string SqlTableName => "dbo.AssetCounterpartymark";
        public string FileNameSubString => "Results";
        public int HeaderRow => 19;
        public string Counterparty => "BANK OF AMERICA, N.A.";
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
                    xl.SetHeaderRow(2, "ML Ref.");
                    var dateString = (string)xl.GetCell(xl.HeaderRow - 4, 4);
                    string company = (string)xl.GetCell(4, 4);
                    var valDate = Extensions.Extensions.GetFirstDateFromString(dateString, @"\d{1,2}-[a-zA-Z]{3}-\d{2}$", "d-MMM-yy") ?? throw new Exception();
                    xl.SetDataTableFormat(outputData);
                    string fileName = OutputPath + "BAML " + valDate.ToString("yyyyMMdd") + company + " CollateralReport" + Path.GetExtension(file);
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
            outputData.Columns.Add(@"notional", typeof(decimal));
            outputData.Columns.Add(@"Buy/Sell", typeof(string));
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["Estimated mid-market value"] == DBNull.Value)
                {
                    row.Delete();
                }
                else 
                {
                    row["valdate"] = asOfDate;
                    row["counterparty"] = Counterparty;
                    row["company"] = company;
                    row["notional"] = row.Field<decimal>("position") * row.Field<decimal>("strike");

                    if (row.Field<decimal>("position") >0)
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
                    sqlCommand.Parameters["@IssuerId"].Value = row.Field<string>("Instrument Name");
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
            tbl.Columns.Add(@"ML Ref.", typeof(string));
            tbl.Columns.Add(@"Entity", typeof(string));
            tbl.Columns.Add(@"Instrument Type", typeof(string));
            tbl.Columns.Add(@"Trade Date", typeof(DateTime));
            tbl.Columns.Add(@"Maturity Date", typeof(DateTime));
            tbl.Columns.Add(@"Instrument Name", typeof(string));
            tbl.Columns.Add(@"Sett. Cur.", typeof(string));
            tbl.Columns.Add(@"Underlying Id", typeof(string));
            tbl.Columns.Add(@"Underlying Name", typeof(string));
            tbl.Columns.Add(@"Underlying Reference Price", typeof(decimal));
            tbl.Columns.Add(@"Call/Put", typeof(string));
            tbl.Columns.Add(@"Strike", typeof(decimal));
            tbl.Columns.Add(@"Position", typeof(decimal));
            tbl.Columns.Add(@"Multiplier", typeof(decimal));
            tbl.Columns.Add(@"Estimated mid-market price", typeof(decimal));
            tbl.Columns.Add(@"Estimated mid-market value", typeof(decimal));
            tbl.Columns.Add(@"MTM FX Rate", typeof(decimal));
            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("valdate", "ValDate");
            sqlBulkCopy.ColumnMappings.Add("company", "CompanyCode");
            sqlBulkCopy.ColumnMappings.Add("Instrument Name", "IssuerId");
            sqlBulkCopy.ColumnMappings.Add("counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "TradeDate");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "ExpirationDate");
            sqlBulkCopy.ColumnMappings.Add("Strike", "Strike");
            sqlBulkCopy.ColumnMappings.Add("Position", "Units");
            sqlBulkCopy.ColumnMappings.Add("notional", "Notional");
            sqlBulkCopy.ColumnMappings.Add(@"Buy/Sell", "Buy_Sell");
            sqlBulkCopy.ColumnMappings.Add("Estimated mid-market value", "MarketValue");
            sqlBulkCopy.ColumnMappings.Add("Underlying Name", "UnderlyingIndex");
            sqlBulkCopy.ColumnMappings.Add("Instrument Type", "OptionType");
            
        }
    }
}