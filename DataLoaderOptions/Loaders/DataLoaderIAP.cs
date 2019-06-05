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

namespace DataLoaderOptions.Loaders
{
    class DataLoaderIAP : DataLoader
    {
        string _folderPath;
        bool _hasHeaders;
        private List<IFixedWidthField> _fields = new List<IFixedWidthField>();

        public override string SqlTableName => "GIS.PoliciesIAP";

        public DataLoaderIAP()
        {
            _folderPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\IAP\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\IAP\\";
            _hasHeaders = false;
        }

        public override void LoadToSql()
        {
            string[] files = Directory.GetFiles(_folderPath, "*", System.IO.SearchOption.TopDirectoryOnly);
            _fields = GetFields();
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();
                DateTime? date = Extensions.Extensions.GetFirstDateFromString(file, @"\d{8}", "yyyyMMdd");
                if (date != null)
                {
                    DateTime asOfDate = (DateTime)date;
                    FillDataTable(outputData, GetConnString(file, true), asOfDate);
                    outputData.AcceptChanges();
                    string fileName = OutputPath + Path.GetFileName(file);
                    AddLoadDetails(outputData, fileName);
                    base.LoadData(outputData);
                    try
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            using (SqlCommand cmd = new SqlCommand("GIS.InsertIAP", con))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.CommandTimeout = 0;
                                cmd.ExecuteNonQuery();
                            }
                            con.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    if (File.Exists(fileName))
                    {
                        File.Delete(fileName);
                    }
                    File.Move(file, fileName);
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("InforceDate", typeof(DateTime));
            tbl.Columns.Add("CO-CODE", typeof(string));
            tbl.Columns.Add("CON-PLANCODE", typeof(string));
            tbl.Columns.Add("GENDER", typeof(int));
            tbl.Columns.Add("ISS-YR", typeof(int));
            tbl.Columns.Add("DURATION", typeof(int));
            tbl.Columns.Add("ISS-AGE", typeof(int));
            tbl.Columns.Add("ISS-MTH", typeof(int));
            tbl.Columns.Add("ISS-DAY", typeof(int));
            tbl.Columns.Add("PLAN-TYPE", typeof(string));
            tbl.Columns.Add("CON-NUM", typeof(string));
            tbl.Columns.Add("REC-TYPE", typeof(int));
            tbl.Columns.Add("REC-INFO", typeof(int));
            tbl.Columns.Add("FUND-CODE", typeof(string));
            tbl.Columns.Add("BASE-AMOUNT", typeof(decimal));
            tbl.Columns.Add("START-DATE", typeof(string));
            tbl.Columns.Add("GTD-MIN-VAL", typeof(decimal));
            tbl.Columns.Add("INDEX-VALUE", typeof(decimal));
            tbl.Columns.Add("PART-RATEOVERRIDE", typeof(decimal));
            tbl.Columns.Add("SPREADOVERRIDE", typeof(decimal));
            tbl.Columns.Add("EARNINGS-CAPOVERRIDE", typeof(decimal));
            tbl.Columns.Add("EARNINGSFLOOROVERRIDE", typeof(decimal));
            tbl.Columns.Add("PREM-RECEIPTDATE", typeof(string));
            tbl.Columns.Add("PREM-PAY-AMT", typeof(decimal));
            tbl.Columns.Add("MIN-BINARYRATE-OVERRIDE", typeof(decimal));
            tbl.Columns.Add("TTL-PART-WITH", typeof(decimal));
            tbl.Columns.Add("BEG-INDEXVALUE", typeof(decimal));
            tbl.Columns.Add("SPREADOVERRIDE-SW", typeof(decimal));
            tbl.Columns.Add("EARNINGSFLOOROVERRIDE-SW", typeof(decimal));
            tbl.Columns.Add("OVERRIDEPERIOD", typeof(int));
            tbl.Columns.Add("THRESHOLDOVERRIDE", typeof(decimal));
            tbl.Columns.Add("PREM-PCTOVERRIDE", typeof(decimal));
            tbl.Columns.Add("ACCUM-RATEOVERRIDE", typeof(decimal));
            tbl.Columns.Add("CURR-OPT-METH", typeof(int));
            tbl.Columns.Add("CURR-OPTVALUE", typeof(decimal));
            tbl.Columns.Add("EQUITY-ALLOCPCT", typeof(decimal));
            tbl.Columns.Add("VOLATILITYRATE-OVERRIDEBEN-OPT", typeof(decimal));
            tbl.Columns.Add("RISK-FREE-RATEOVERRIDE", typeof(decimal));
            tbl.Columns.Add("DIVIDEND-RATEOVERRIDE", typeof(decimal));
            tbl.Columns.Add("VOLATILITYRATE-OVERRIDECAP-OPT", typeof(decimal));


            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("CO-CODE", "CoCode");
            sqlBulkCopy.ColumnMappings.Add("CON-PLANCODE", "PlanCode");
            sqlBulkCopy.ColumnMappings.Add("GENDER", "Gender");
            sqlBulkCopy.ColumnMappings.Add("ISS-YR", "IssueYear");
            sqlBulkCopy.ColumnMappings.Add("DURATION", "Duration");
            sqlBulkCopy.ColumnMappings.Add("ISS-AGE", "IssueAge");
            sqlBulkCopy.ColumnMappings.Add("ISS-MTH", "IssueMonth");
            sqlBulkCopy.ColumnMappings.Add("ISS-DAY", "IssueDay");
            sqlBulkCopy.ColumnMappings.Add("PLAN-TYPE", "PlanType");
            sqlBulkCopy.ColumnMappings.Add("CON-NUM", "ConNumber");
            sqlBulkCopy.ColumnMappings.Add("REC-TYPE", "RecType");
            sqlBulkCopy.ColumnMappings.Add("REC-INFO", "RecInfo");
            sqlBulkCopy.ColumnMappings.Add("FUND-CODE", "FundCode");
            sqlBulkCopy.ColumnMappings.Add("BASE-AMOUNT", "BaseAmount");
            sqlBulkCopy.ColumnMappings.Add("START-DATE", "StartDate");
            sqlBulkCopy.ColumnMappings.Add("GTD-MIN-VAL", "GtdMinVal");
            sqlBulkCopy.ColumnMappings.Add("INDEX-VALUE", "IndexValue");
            sqlBulkCopy.ColumnMappings.Add("PART-RATEOVERRIDE", "PartRateOverride");
            sqlBulkCopy.ColumnMappings.Add("SPREADOVERRIDE", "SpreadOverride");
            sqlBulkCopy.ColumnMappings.Add("EARNINGS-CAPOVERRIDE", "EarningsCapOverride");
            sqlBulkCopy.ColumnMappings.Add("EARNINGSFLOOROVERRIDE", "EarningsFloorOverride");
            sqlBulkCopy.ColumnMappings.Add("PREM-RECEIPTDATE", "PremReceiptDate");
            sqlBulkCopy.ColumnMappings.Add("PREM-PAY-AMT", "PremPayAmt");
            sqlBulkCopy.ColumnMappings.Add("MIN-BINARYRATE-OVERRIDE", "MinBinaryRateOverride");
            sqlBulkCopy.ColumnMappings.Add("TTL-PART-WITH", "TTLPartWith");
            sqlBulkCopy.ColumnMappings.Add("BEG-INDEXVALUE", "BegIndexValue");
            sqlBulkCopy.ColumnMappings.Add("SPREADOVERRIDE-SW", "SpreadOverrideSw");
            sqlBulkCopy.ColumnMappings.Add("EARNINGSFLOOROVERRIDE-SW", "EarningsFloorOverrideSw");
            sqlBulkCopy.ColumnMappings.Add("OVERRIDEPERIOD", "OverridePeriod");
            sqlBulkCopy.ColumnMappings.Add("THRESHOLDOVERRIDE", "ThresholdOverride");
            sqlBulkCopy.ColumnMappings.Add("PREM-PCTOVERRIDE", "PremPctOverride");
            sqlBulkCopy.ColumnMappings.Add("ACCUM-RATEOVERRIDE", "AccumRateOverride");
            sqlBulkCopy.ColumnMappings.Add("CURR-OPT-METH", "CurrOptMeth");
            sqlBulkCopy.ColumnMappings.Add("CURR-OPTVALUE", "CurrOptValue");
            sqlBulkCopy.ColumnMappings.Add("EQUITY-ALLOCPCT", "EquityAllocPct");
            sqlBulkCopy.ColumnMappings.Add("VOLATILITYRATE-OVERRIDEBEN-OPT", "VolatilityRateOverrideBenOpt");
            sqlBulkCopy.ColumnMappings.Add("RISK-FREE-RATEOVERRIDE", "RiskFreeRateOverride");
            sqlBulkCopy.ColumnMappings.Add("DIVIDEND-RATEOVERRIDE", "DividendRateOverride");
            sqlBulkCopy.ColumnMappings.Add("VOLATILITYRATE-OVERRIDECAP-OPT", "VolatilityRateOverrideCapOpt");

        }
        private List<IFixedWidthField> GetFields()
        {
            List<IFixedWidthField> list = new List<IFixedWidthField>();
            list.Add(new FixedWidthField_String(0, 2, "CO-CODE"));
            list.Add(new FixedWidthField_String(2, 12, "CON-PLANCODE"));
            list.Add(new FixedWidthField_Int(14, 1, "GENDER"));
            list.Add(new FixedWidthField_Int(15, 4, "ISS-YR"));
            list.Add(new FixedWidthField_Int(19, 2, "DURATION"));
            list.Add(new FixedWidthField_Int(21, 3, "ISS-AGE"));
            list.Add(new FixedWidthField_Int(24, 2, "ISS-MTH"));
            list.Add(new FixedWidthField_Int(26, 2, "ISS-DAY"));
            list.Add(new FixedWidthField_String(28, 1, "PLAN-TYPE"));
            list.Add(new FixedWidthField_String(30, 12, "CON-NUM"));
            list.Add(new FixedWidthField_Int(42, 1, "REC-TYPE"));
            list.Add(new FixedWidthField_Int(43, 1, "REC-INFO"));
            list.Add(new FixedWidthField_String(44, 5, "FUND-CODE"));
            list.Add(new FixedWidthField_Decimal(49, 12, 2, "BASE-AMOUNT"));
            list.Add(new FixedWidthField_String(61, 8, "START-DATE"));
            list.Add(new FixedWidthField_Decimal(69, 12, 2, "GTD-MIN-VAL"));
            list.Add(new FixedWidthField_Decimal(81, 12, 2, "INDEX-VALUE"));
            list.Add(new FixedWidthField_Decimal(93, 6, 5, "PART-RATEOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(99, 6, 5, "SPREADOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(105, 6, 5, "EARNINGS-CAPOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(111, 6, 5, "EARNINGSFLOOROVERRIDE"));
            list.Add(new FixedWidthField_String(117, 8, "PREM-RECEIPTDATE"));
            list.Add(new FixedWidthField_Decimal(125, 12, 2, "PREM-PAY-AMT"));
            list.Add(new FixedWidthField_Decimal(137, 5, 5, "MIN-BINARYRATE-OVERRIDE"));
            list.Add(new FixedWidthField_Decimal(142, 12, 2, "TTL-PART-WITH"));
            list.Add(new FixedWidthField_Decimal(154, 12, 2, "BEG-INDEXVALUE"));
            list.Add(new FixedWidthField_Decimal(166, 1, 0, "SPREADOVERRIDE-SW"));
            list.Add(new FixedWidthField_Decimal(167, 1, 0, "EARNINGSFLOOROVERRIDE-SW"));
            list.Add(new FixedWidthField_Int(168, 2, "OVERRIDEPERIOD"));
            list.Add(new FixedWidthField_Decimal(170, 6, 5, "THRESHOLDOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(176, 6, 5, "PREM-PCTOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(182, 6, 5, "ACCUM-RATEOVERRIDE"));
            list.Add(new FixedWidthField_Int(188, 1, "CURR-OPT-METH"));
            list.Add(new FixedWidthField_Decimal(189, 9, 8, "CURR-OPTVALUE"));
            list.Add(new FixedWidthField_Decimal(198, 6, 5, "EQUITY-ALLOCPCT"));
            list.Add(new FixedWidthField_Decimal(204, 6, 5, "VOLATILITYRATE-OVERRIDEBEN-OPT"));
            list.Add(new FixedWidthField_Decimal(210, 6, 5, "RISK-FREE-RATEOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(216, 6, 5, "DIVIDEND-RATEOVERRIDE"));
            list.Add(new FixedWidthField_Decimal(222, 6, 5, "VOLATILITYRATE-OVERRIDECAP-OPT"));
            return list;
        }
        protected void AddLoadDetails(DataTable outputData, string source)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("Source", typeof(string));
            outputData.Columns.Add("UserID", typeof(string));
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["LoadDate"] = loadDate;
                row["UserID"] = Environment.UserName;
                row["Source"] = source;
            }
            outputData.AcceptChanges();
        }
        protected void FillDataTable(DataTable outputData, string conString, DateTime asOfDate)
        {
            using (TextReader reader = new StreamReader(conString))
            {
                while (true)
                {
                    string newLine = reader.ReadLine();
                    if (newLine == null)
                    {
                        break;
                    }

                    DataRow toInsert = outputData.NewRow();
                    toInsert["InforceDate"] = asOfDate;
                    foreach (IFixedWidthField fld in _fields)
                    {
                        toInsert[fld.ColumnName] = Convert.ChangeType(fld.ReadValue(newLine), fld.DataType);
                    }
                    outputData.Rows.Add(toInsert);
                }
            }
        }
    }
}
