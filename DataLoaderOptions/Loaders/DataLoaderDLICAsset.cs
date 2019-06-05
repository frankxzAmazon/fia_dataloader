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

namespace DataLoaderOptions
{
    class DataLoaderDLICAsset : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 1;
        public DataLoaderDLICAsset()
        {
            inputFolder = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Assets\ToUpload\";
            OutputPath = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Assets\\";
        }

        public override string SqlTableName => "DLIC.DailyAssetReport";
        public void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*.xls", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();

                DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                xl.SetDataTableFormat(outputData);
                outputData = xl.GetFilledDataTable(OnError.UseNullValue);

                CorrectDataTable(outputData, file);
                outputData.AcceptChanges();
                DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("Report Effective Date")).Max();
                string fileName = OutputPath + asOfDate.ToString("yyyyMMdd") + "_" + "Derivatives_Holdings.xls";
                string filepath = OutputPath + Path.GetFileName(file);
                outputData.AcceptChanges();
                lock (toLock)
                {
                    base.LoadData(outputData);
                }
                if (ToLoad)
                {
                    try
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            using (SqlCommand cmd = new SqlCommand("DLIC.InsertAssets", con))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.CommandTimeout = 0;
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                File.Move(file, fileName);
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("Report Effective Date", typeof(DateTime));
            tbl.Columns.Add("Deal Functional Currency Code", typeof(string));
            tbl.Columns.Add("Investment Sub Legal Entity Code", typeof(string));
            tbl.Columns.Add("Business Group Code", typeof(string));
            tbl.Columns.Add("Desk", typeof(string));
            tbl.Columns.Add("Deal Type", typeof(string));
            tbl.Columns.Add("Deal Id", typeof(string));
            tbl.Columns.Add("Trade Date", typeof(DateTime));
            tbl.Columns.Add("Effective Date", typeof(DateTime));
            tbl.Columns.Add("Maturity Date", typeof(DateTime));
            tbl.Columns.Add("Expiry Date", typeof(DateTime));
            tbl.Columns.Add("Unit Quantity", typeof(decimal));
            tbl.Columns.Add("Current Notional Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Unadjusted Market Value Amt Func CCY (excl. Accruals)", typeof(decimal));
            tbl.Columns.Add("Credit Value Adjustment Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Market Value Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Pay Accrued Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Receive Accrued Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Counter Party Name", typeof(string));
            tbl.Columns.Add("Internal Credit Rating Code", typeof(string));
            tbl.Columns.Add("Executing Broker Name", typeof(string));
            tbl.Columns.Add("Clearing Broker Name", typeof(string));
            tbl.Columns.Add("Clearing House Name", typeof(string));
            tbl.Columns.Add("Exchange Traded (ET) or OTC", typeof(string));
            tbl.Columns.Add("ISDA Id", typeof(string));
            tbl.Columns.Add("CSA In Place Ind", typeof(string));
            tbl.Columns.Add("Pay Interest Rate Type Code", typeof(string));
            tbl.Columns.Add("Pay Original Currency Code", typeof(string));
            tbl.Columns.Add("Pay Current Notional Amt Orig CCY", typeof(decimal));
            tbl.Columns.Add("Pay Original Notional Amt Orig CCY", typeof(decimal));
            tbl.Columns.Add("Receive Interest Rate Type Code", typeof(string));
            tbl.Columns.Add("Receive Original Currency Code", typeof(string));
            tbl.Columns.Add("Receive Current Notional Amt Orig CCY", typeof(decimal));
            tbl.Columns.Add("Receive Original Notional Amt Orig CCY", typeof(decimal));
            tbl.Columns.Add("Pay Interest Payment Day", typeof(int));
            tbl.Columns.Add("Pay Payment Frequency Code", typeof(string));
            tbl.Columns.Add("Pay Benchmark Rate Name", typeof(string));
            tbl.Columns.Add("Pay Interest Reset Period", typeof(string));
            tbl.Columns.Add("Pay Interest Rate Basis Point Spread", typeof(string));
            tbl.Columns.Add("Pay Benchmark Rate %", typeof(string));
            tbl.Columns.Add("Pay Interest Rate Compound Frequency Code", typeof(string));
            tbl.Columns.Add("Receive Interest Payment Day", typeof(string));
            tbl.Columns.Add("Receive Payment Frequency Code", typeof(string));
            tbl.Columns.Add("Receive Benchmark Rate Name", typeof(string));
            tbl.Columns.Add("Receive Interest Reset Period", typeof(string));
            tbl.Columns.Add("Receive Interest Rate Basis Point Spread", typeof(string));
            tbl.Columns.Add("Receive Benchmark Rate %", typeof(string));
            tbl.Columns.Add("Receive Interest Rate Compound Frequency Code", typeof(string));
            tbl.Columns.Add("IGAAP Treatment Hedge Type Desc", typeof(string));
            tbl.Columns.Add("Strategy Desc", typeof(string));
            tbl.Columns.Add("Common Reference", typeof(string));
            tbl.Columns.Add("Reference Index Id", typeof(string));
            tbl.Columns.Add("Exchange Name", typeof(string));
            tbl.Columns.Add("Initial Price", typeof(decimal));
            tbl.Columns.Add("Current Price", typeof(decimal));
            tbl.Columns.Add("Strike Rate", typeof(decimal));
            tbl.Columns.Add("Multiplier", typeof(int));
            tbl.Columns.Add("Option Put / Call Code", typeof(string));
            tbl.Columns.Add("IGAAP Cost/Premium Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Cost per Unit Orig CCY", typeof(decimal));
            tbl.Columns.Add("IGAAP Unamortize Cost Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("IGAAP Life-to-Date Amortize Amt Func CCY", typeof(decimal));
            tbl.Columns.Add("Averaging Day", typeof(int));
            tbl.Columns.Add("Average Term Months", typeof(int));
            tbl.Columns.Add("Cap Strike Rate", typeof(decimal));
            tbl.Columns.Add("Derivatives Duration", typeof(decimal));
            tbl.Columns.Add("Segment Acronymn Code", typeof(string));
            tbl.Columns.Add("Book", typeof(string));
            tbl.Columns.Add("External Trade Id", typeof(string));
            tbl.Columns.Add("MUST Template", typeof(string));
            tbl.Columns.Add("Data Load Source", typeof(string));
            tbl.Columns.Add("Deal Component Augmented Ind", typeof(string));
            tbl.Columns.Add("Fusion LE", typeof(string));
            tbl.Columns.Add("Fusion MP", typeof(string));
            tbl.Columns.Add("Fusion Key", typeof(string));

            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, string source)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("Source", typeof(string));
            outputData.Columns.Add("UserId", typeof(string));
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["Report Effective Date"] == DBNull.Value)
                {
                    row.Delete();
                }
                else
                {
                    row["LoadDate"] = loadDate;
                    row["UserID"] = Environment.UserName;
                    row["Source"] = source;
                }
            }
            outputData.AcceptChanges();
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserID", "UserID");
            sqlBulkCopy.ColumnMappings.Add("Report Effective Date", "Report Effective Date");
            sqlBulkCopy.ColumnMappings.Add("Deal Functional Currency Code", "Deal Functional Currency Code");
            sqlBulkCopy.ColumnMappings.Add("Investment Sub Legal Entity Code", "Investment Sub Legal Entity Code");
            sqlBulkCopy.ColumnMappings.Add("Business Group Code", "Business Group Code");
            sqlBulkCopy.ColumnMappings.Add("Desk", "Desk");
            sqlBulkCopy.ColumnMappings.Add("Deal Type", "Deal Type");
            sqlBulkCopy.ColumnMappings.Add("Deal Id", "Deal Id");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "Trade Date");
            sqlBulkCopy.ColumnMappings.Add("Effective Date", "Effective Date");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "Maturity Date");
            sqlBulkCopy.ColumnMappings.Add("Expiry Date", "Expiry Date");
            sqlBulkCopy.ColumnMappings.Add("Unit Quantity", "Unit Quantity");
            sqlBulkCopy.ColumnMappings.Add("Current Notional Amt Func CCY", "Current Notional Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Unadjusted Market Value Amt Func CCY (excl. Accruals)", "Unadjusted Market Value Amt Func CCY (excl. Accruals)");
            sqlBulkCopy.ColumnMappings.Add("Credit Value Adjustment Amt Func CCY", "Credit Value Adjustment Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Market Value Amt Func CCY", "Market Value Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Pay Accrued Amt Func CCY", "Pay Accrued Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Receive Accrued Amt Func CCY", "Receive Accrued Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Counter Party Name", "Counter Party Name");
            sqlBulkCopy.ColumnMappings.Add("Internal Credit Rating Code", "Internal Credit Rating Code");
            sqlBulkCopy.ColumnMappings.Add("Executing Broker Name", "Executing Broker Name");
            sqlBulkCopy.ColumnMappings.Add("Clearing Broker Name", "Clearing Broker Name");
            sqlBulkCopy.ColumnMappings.Add("Clearing House Name", "Clearing House Name");
            sqlBulkCopy.ColumnMappings.Add("Exchange Traded (ET) or OTC", "Exchange Traded (ET) or OTC");
            sqlBulkCopy.ColumnMappings.Add("ISDA Id", "ISDA Id");
            sqlBulkCopy.ColumnMappings.Add("CSA In Place Ind", "CSA In Place Ind");
            sqlBulkCopy.ColumnMappings.Add("Pay Interest Rate Type Code", "Pay Interest Rate Type Code");
            sqlBulkCopy.ColumnMappings.Add("Pay Original Currency Code", "Pay Original Currency Code");
            sqlBulkCopy.ColumnMappings.Add("Pay Current Notional Amt Orig CCY", "Pay Current Notional Amt Orig CCY");
            sqlBulkCopy.ColumnMappings.Add("Pay Original Notional Amt Orig CCY", "Pay Original Notional Amt Orig CCY");
            sqlBulkCopy.ColumnMappings.Add("Receive Interest Rate Type Code", "Receive Interest Rate Type Code");
            sqlBulkCopy.ColumnMappings.Add("Receive Original Currency Code", "Receive Original Currency Code");
            sqlBulkCopy.ColumnMappings.Add("Receive Current Notional Amt Orig CCY", "Receive Current Notional Amt Orig CCY");
            sqlBulkCopy.ColumnMappings.Add("Receive Original Notional Amt Orig CCY", "Receive Original Notional Amt Orig CCY");
            sqlBulkCopy.ColumnMappings.Add("Pay Interest Payment Day", "Pay Interest Payment Day");
            sqlBulkCopy.ColumnMappings.Add("Pay Payment Frequency Code", "Pay Payment Frequency Code");
            sqlBulkCopy.ColumnMappings.Add("Pay Benchmark Rate Name", "Pay Benchmark Rate Name");
            sqlBulkCopy.ColumnMappings.Add("Pay Interest Reset Period", "Pay Interest Reset Period");
            sqlBulkCopy.ColumnMappings.Add("Pay Interest Rate Basis Point Spread", "Pay Interest Rate Basis Point Spread");
            sqlBulkCopy.ColumnMappings.Add("Pay Benchmark Rate %", "Pay Benchmark Rate %");
            sqlBulkCopy.ColumnMappings.Add("Pay Interest Rate Compound Frequency Code", "Pay Interest Rate Compound Frequency Code");
            sqlBulkCopy.ColumnMappings.Add("Receive Interest Payment Day", "Receive Interest Payment Day");
            sqlBulkCopy.ColumnMappings.Add("Receive Payment Frequency Code", "Receive Payment Frequency Code");
            sqlBulkCopy.ColumnMappings.Add("Receive Benchmark Rate Name", "Receive Benchmark Rate Name");
            sqlBulkCopy.ColumnMappings.Add("Receive Interest Reset Period", "Receive Interest Reset Period");
            sqlBulkCopy.ColumnMappings.Add("Receive Interest Rate Basis Point Spread", "Receive Interest Rate Basis Point Spread");
            sqlBulkCopy.ColumnMappings.Add("Receive Benchmark Rate %", "Receive Benchmark Rate %");
            sqlBulkCopy.ColumnMappings.Add("Receive Interest Rate Compound Frequency Code", "Receive Interest Rate Compound Frequency Code");
            sqlBulkCopy.ColumnMappings.Add("IGAAP Treatment Hedge Type Desc", "IGAAP Treatment Hedge Type Desc");
            sqlBulkCopy.ColumnMappings.Add("Strategy Desc", "Strategy Desc");
            sqlBulkCopy.ColumnMappings.Add("Common Reference", "Common Reference");
            sqlBulkCopy.ColumnMappings.Add("Reference Index Id", "Reference Index Id");
            sqlBulkCopy.ColumnMappings.Add("Exchange Name", "Exchange Name");
            sqlBulkCopy.ColumnMappings.Add("Initial Price", "Initial Price");
            sqlBulkCopy.ColumnMappings.Add("Current Price", "Current Price");
            sqlBulkCopy.ColumnMappings.Add("Strike Rate", "Strike Rate");
            sqlBulkCopy.ColumnMappings.Add("Multiplier", "Multiplier");
            sqlBulkCopy.ColumnMappings.Add("Option Put / Call Code", "Option Put / Call Code");
            sqlBulkCopy.ColumnMappings.Add("IGAAP Cost/Premium Amt Func CCY", "IGAAP Cost/Premium Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Cost per Unit Orig CCY", "Cost per Unit Orig CCY");
            sqlBulkCopy.ColumnMappings.Add("IGAAP Unamortize Cost Amt Func CCY", "IGAAP Unamortize Cost Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("IGAAP Life-to-Date Amortize Amt Func CCY", "IGAAP Life-to-Date Amortize Amt Func CCY");
            sqlBulkCopy.ColumnMappings.Add("Averaging Day", "Averaging Day");
            sqlBulkCopy.ColumnMappings.Add("Average Term Months", "Average Term Months");
            sqlBulkCopy.ColumnMappings.Add("Cap Strike Rate", "Cap Strike Rate");
            sqlBulkCopy.ColumnMappings.Add("Derivatives Duration", "Derivatives Duration");
            sqlBulkCopy.ColumnMappings.Add("Segment Acronymn Code", "Segment Acronymn Code");
            sqlBulkCopy.ColumnMappings.Add("Book", "Book");
            sqlBulkCopy.ColumnMappings.Add("External Trade Id", "External Trade Id");
            sqlBulkCopy.ColumnMappings.Add("MUST Template", "MUST Template");
            sqlBulkCopy.ColumnMappings.Add("Data Load Source", "Data Load Source");
            sqlBulkCopy.ColumnMappings.Add("Deal Component Augmented Ind", "Deal Component Augmented Ind");
            sqlBulkCopy.ColumnMappings.Add("Fusion LE", "Fusion LE");
            sqlBulkCopy.ColumnMappings.Add("Fusion MP", "Fusion MP");
            sqlBulkCopy.ColumnMappings.Add("Fusion Key", "Fusion Key");

        }
    }
}
