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
    class DataLoaderDLICFrontOffice : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        int headerRow = 2;
        public DataLoaderDLICFrontOffice()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA Assets\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA Assets\\";
        }

        public override string SqlTableName => "DLIC.FrontOfficeHoldingsReport";
        public void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*Front Office*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();

                DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                xl.SetDataTableFormat(outputData);
                outputData = xl.GetFilledDataTable(OnError.UseNullValue);
                DateTime asOfDate = DateTime.Parse(xl.GetCell(1, 4).ToString());
                CorrectDataTable(outputData, asOfDate);
                outputData.AcceptChanges();
                
                string fileName = Path.GetFileName(file);
                if (!File.Exists(fileName))
                {
                    string filepath = OutputPath + Path.GetFileName(file);
                    outputData.AcceptChanges();
                    lock (toLock)
                    {
                        base.LoadData(outputData);
                    }
                    File.Copy(file, fileName);
                    File.Delete(file);
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("InforceDate", typeof(DateTime));
            tbl.Columns.Add("Legal Entity", typeof(string));
            tbl.Columns.Add("Segment", typeof(string));
            tbl.Columns.Add("Deal Type", typeof(string));
            tbl.Columns.Add("Trade ID", typeof(string));
            tbl.Columns.Add("Counterparty", typeof(string));
            tbl.Columns.Add("Trade Date", typeof(DateTime));
            tbl.Columns.Add("Effective Date", typeof(DateTime));
            tbl.Columns.Add("Maturity Date", typeof(DateTime));
            tbl.Columns.Add("Expiry Date", typeof(DateTime));
            tbl.Columns.Add("Pay Notional", typeof(decimal));
            tbl.Columns.Add("Pay Ccy", typeof(string));
            tbl.Columns.Add("PayRateIndexFreq", typeof(string));
            tbl.Columns.Add("Rec Notional", typeof(decimal));
            tbl.Columns.Add("Rec Ccy", typeof(string));
            tbl.Columns.Add("RecRateIndexFreq", typeof(string));
            tbl.Columns.Add("Market Value", typeof(decimal));
            tbl.Columns.Add("Functional Ccy", typeof(string));
            tbl.Columns.Add("Strike Rate", typeof(decimal));
            tbl.Columns.Add("Long/Short/Buy/Sell", typeof(string));
            tbl.Columns.Add("Option Type", typeof(string));
            tbl.Columns.Add("Put/Call", typeof(string));
            tbl.Columns.Add("Units", typeof(decimal));
            tbl.Columns.Add("Ref Index", typeof(string));
            tbl.Columns.Add("Delta", typeof(decimal));
            tbl.Columns.Add("Vega", typeof(decimal));
            tbl.Columns.Add("Rho", typeof(decimal));
            tbl.Columns.Add("PV01", typeof(decimal));
            tbl.Columns.Add("Deriv Gamma Rate", typeof(decimal));
            tbl.Columns.Add("Deriv Gamma PV1", typeof(decimal));
            tbl.Columns.Add("External Trade Id", typeof(string));
            tbl.Columns.Add("Strategy Desc", typeof(string));


            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData, DateTime date)
        {
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                if (row["Trade ID"] == DBNull.Value)
                {
                    row.Delete();
                }
                else
                {
                    row["InforceDate"] = date;
                }
            }
            outputData.AcceptChanges();
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("Legal Entity", "Legal Entity");
            sqlBulkCopy.ColumnMappings.Add("Segment", "Segment");
            sqlBulkCopy.ColumnMappings.Add("Deal Type", "Deal Type");
            sqlBulkCopy.ColumnMappings.Add("Trade ID", "Trade ID");
            sqlBulkCopy.ColumnMappings.Add("Counterparty", "Counterparty");
            sqlBulkCopy.ColumnMappings.Add("Trade Date", "Trade Date");
            sqlBulkCopy.ColumnMappings.Add("Effective Date", "Effective Date");
            sqlBulkCopy.ColumnMappings.Add("Maturity Date", "Maturity Date");
            sqlBulkCopy.ColumnMappings.Add("Expiry Date", "Expiry Date");
            sqlBulkCopy.ColumnMappings.Add("Pay Notional", "Pay Notional");
            sqlBulkCopy.ColumnMappings.Add("Pay Ccy", "Pay Ccy");
            sqlBulkCopy.ColumnMappings.Add("PayRateIndexFreq", "PayRateIndexFreq");
            sqlBulkCopy.ColumnMappings.Add("Rec Notional", "Rec Notional");
            sqlBulkCopy.ColumnMappings.Add("Rec Ccy", "Rec Ccy");
            sqlBulkCopy.ColumnMappings.Add("RecRateIndexFreq", "RecRateIndexFreq");
            sqlBulkCopy.ColumnMappings.Add("Market Value", "Market Value");
            sqlBulkCopy.ColumnMappings.Add("Functional Ccy", "Functional Ccy");
            sqlBulkCopy.ColumnMappings.Add("Strike Rate", "Strike Rate");
            sqlBulkCopy.ColumnMappings.Add("Long/Short/Buy/Sell", "Long/Short/Buy/Sell");
            sqlBulkCopy.ColumnMappings.Add("Option Type", "Option Type");
            sqlBulkCopy.ColumnMappings.Add("Put/Call", "Put/Call");
            sqlBulkCopy.ColumnMappings.Add("Units", "Units");
            sqlBulkCopy.ColumnMappings.Add("Ref Index", "Ref Index");
            sqlBulkCopy.ColumnMappings.Add("Delta", "Delta");
            sqlBulkCopy.ColumnMappings.Add("Vega", "Vega");
            sqlBulkCopy.ColumnMappings.Add("Rho", "Rho");
            sqlBulkCopy.ColumnMappings.Add("PV01", "PV01");
            sqlBulkCopy.ColumnMappings.Add("Deriv Gamma Rate", "Deriv Gamma Rate");
            sqlBulkCopy.ColumnMappings.Add("Deriv Gamma PV1", "Deriv Gamma PV1");
            sqlBulkCopy.ColumnMappings.Add("External Trade Id", "External Trade Id");
            sqlBulkCopy.ColumnMappings.Add("Strategy Desc", "Strategy Desc");


        }
    }
}
