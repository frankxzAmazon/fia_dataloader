using CsvHelper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;

namespace DataLoaderOptions
{
    class DataLoaderDLICNbFIA : DataLoader
    {
        Dictionary<string, string> FundType = new Dictionary<string, string>()
        {
            {" DE FIA CROCI Annual Pt to Pt w/Sp 7 Yr", "Spread Rate"},
            {" DE FIA MAA 2 Year PtP w/Part Rate 7 Yr", "Part Rate"},
            {" DE FIA MAA Annual PtP w/Part Rate 7 Yr", "Part Rate"},
            {" DE FIA S&P Annual PtP w/ Cap 7 Yr", "Cap Rate"},
            {" DE FIA S&P Monthly Avg w/Part Rate 7 Yr", "Part Rate"},
            {"DE AI7 S&P Ann PtP w/Part Rate", "Part Rate"},
            {"DE AI7 S&P500 Annual Perf Trigger", "Performance Trigger Rate"},
            {"DE FIA AI7 S&P Annual PtP w/ Cap 7 Yr", "Cap Rate"},
            {"DE FIA CROCI Ann Pt to Pt w/Sp 7 Yr w/BO", "Spread Rate"},
            {"DE FIA CROCI Annual Pt to Pt w/Spread", "Spread Rate"},
            {"DE FIA MAA 2 Year Pt to Pt w/Part Rate", "Part Rate"},
            {"DE FIA MAA 2 Yr PtP w/PartRate 7 Yr w/BO", "Part Rate"},
            {"DE FIA MAA Ann PtP w/Part Rate 7 Yr w/BO", "Part Rate"},
            {"DE FIA MAA Annual Pt to Pt w/Part Rate", "Part Rate"},
            {"DE FIA S&P Annual Pt to Pt with Cap", "Cap Rate"},
            {"DE FIA S&P Annual PtP w/ Cap 7 Yr w/BO", "Cap Rate"},
            {"DE FIA S&P Monthly Avg w/Part Rate", "Part Rate"},
            {"DE FIA S&P Monthy Pt to Pt w/Cap", "Cap Rate"},
            {"DE RC10 S&P PtP w/Part Rate", "Part Rate"},
            {"DE RS7 S&P Ann PtP w/Part Rate", "Part Rate"},
            {"DE RS7 S&P500 Ann Perf Trig", "Performance Trigger Rate"},
            {" DE FIA Fixed Account 7 Yr", ""},
            {"DE AI7 FIA Fixed Acct", ""},
            {"DE FIA Fixed Account 10 Yr", ""},
            {"DE FIA Fixed Account 7 Yr w/BO", ""}

        };
        string inputFolder;
        static object toLock = new object();
        int headerRow = 1;
        public DataLoaderDLICNbFIA()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA\\";
        }

        public override string SqlTableName => "DLIC.PoliciesNbFIA";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();
                DataTable secondSheet = outputData.Clone();
                string infile = OutputPath + Path.GetFileName(file);
                FillDataTable(outputData, GetConnString(file, true), infile);

                // Comment this out: this code is for reading an Excel file
                // Get data from the first sheet
                //DataReader_Excel xl = new DataReader_Excel(file, headerRow);
                //xl.SetDataTableFormat(outputData);
                //outputData = xl.GetFilledDataTable(OnError.UseNullValue);

                //// Get data from the second sheet
                //var colNames = xl.WorksheetColumns;
                //xl = new DataReader_Excel(file, headerRow, 2);
                //xl.WorksheetColumns = colNames;
                //secondSheet = xl.GetFilledDataTable(OnError.UseNullValue, hasHeader: false, template: outputData);

                //// Merge data from the first and second sheets
                //outputData.Merge(secondSheet);



                CorrectDataTable(outputData);
                CorrectCoupon(outputData);
                outputData.AcceptChanges();

                DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("Current Index Date")).Max();
                string outfile = OutputPath + "Rpt_FIA_Invstmnt_Sales_7Yr_10Yr_Combined-" + asOfDate.ToString("yyyyMMdd") + ".csv";
                if (outputData.Rows.Count > 0)
                {
                    //string filepath = OutputPath + Path.GetFileName(file);
                    UpdateAsOfDate(outputData, asOfDate, outfile);
                    outputData.AcceptChanges();
                    lock (toLock)
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Sql"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            using (SqlCommand cmd = new SqlCommand($"delete from {SqlTableName}", con))
                            {
                                //cmd.CommandType = CommandType.Text;
                                cmd.CommandTimeout = 0;
                                cmd.ExecuteNonQuery();
                            }

                            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                            {
                                //Set the database table name
                                sqlBulkCopy.DestinationTableName = SqlTableName;
                                MapTable(sqlBulkCopy);
                                sqlBulkCopy.BulkCopyTimeout = 0;
                                sqlBulkCopy.WriteToServer(outputData);
                            }
                            if (ToLoad)
                            {
                                try
                                {
                                    using (SqlCommand cmd = new SqlCommand("DLIC.InsertNbFIA", con))
                                    {
                                        cmd.CommandType = CommandType.StoredProcedure;
                                        cmd.CommandTimeout = 0;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.Message);
                                }
                            }
                        }
                    }
                    if (File.Exists(outfile))
                    {
                        File.Delete(outfile);
                    }
                    File.Move(file, outfile);
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();

            tbl.Columns.Add("InforceDate", typeof(DateTime));
            tbl.Columns.Add("Policy", typeof(string));
            tbl.Columns.Add("Plan ID", typeof(string));
            tbl.Columns.Add("Duration", typeof(decimal));
            tbl.Columns.Add("Current Index Date", typeof(DateTime));
            tbl.Columns.Add("End Index Date", typeof(DateTime));
            tbl.Columns.Add("Account Value", typeof(decimal));
            tbl.Columns.Add("Raw Premium", typeof(decimal));
            tbl.Columns.Add("Part Rate", typeof(decimal));
            tbl.Columns.Add("Spread Rate", typeof(decimal));
            tbl.Columns.Add("Cap Rate", typeof(decimal));
            tbl.Columns.Add("Floor Rate", typeof(decimal));
            tbl.Columns.Add("Performance Trigger Rate", typeof(decimal));
            tbl.Columns.Add("Beginning Index Value", typeof(decimal));
            tbl.Columns.Add("Issue Date", typeof(string));
            tbl.Columns.Add("Roll Indicator", typeof(string));
            tbl.Columns.Add("Fund Description", typeof(string));
            tbl.Columns.Add("Product Name", typeof(string));
            tbl.Columns.Add("Coupon Rate", typeof(decimal));
            return tbl;
        }

        protected void CorrectCoupon(DataTable outputData)
        {
            foreach (DataRow row in outputData.Rows)
            {
                if ((decimal)row["Coupon Rate"] > 100)
                {
                    row["Coupon Rate"] = (decimal)row["Coupon Rate"] / 100;
                }
            }
        }
        protected void CorrectDataTable(DataTable outputData)
        {
            int i = 0;

            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {

                if (row["Policy"] == DBNull.Value)
                {
                    row.Delete();
                }
                else
                {
                    row["Issue Date"] = DateTime.ParseExact(row["Issue Date"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture).ToString("yyyy-MM-dd");
                    string fundType;
                    if (FundType.ContainsKey((string)row["Fund Description"]))
                    {
                        fundType = FundType[(string)row["Fund Description"]];
                    }
                    else
                    {
                        fundType = "";
                    }
                    if (!string.IsNullOrEmpty(fundType))
                    {
                        decimal value = (decimal)row[fundType];
                        if (value == 0)
                        {
                            if ((decimal)row["Floor Rate"] != 0)
                            {
                                row[fundType] = (decimal)row["Floor Rate"];
                            }
                        }
                        else if (value > 10 && !fundType.Equals("Part Rate", StringComparison.InvariantCultureIgnoreCase))
                        {
                            while (value > 10)
                            {
                                value /= 10;
                            }
                            row[fundType] = value;
                        }
                        else if (value > 200 && fundType.Equals("Part Rate", StringComparison.InvariantCultureIgnoreCase))
                        {
                            while (value > 200)
                            {
                                value /= 10;
                            }
                            row[fundType] = value;
                        }
                        row["Floor Rate"] = 0;
                    }
                    i++;
                }
            }
        }
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string source)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
            outputData.Columns.Add("Source", typeof(string));
            outputData.Columns.Add("UserID", typeof(string));
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {
                row["LoadDate"] = loadDate;
                row["UserID"] = Environment.UserName;
                row["InforceDate"] = asOfDate;
                row["Source"] = source;
            }
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table

            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("Policy", "PolicyNumber");
            sqlBulkCopy.ColumnMappings.Add("Plan ID", "PlanID");
            sqlBulkCopy.ColumnMappings.Add("Duration", "Duration");
            sqlBulkCopy.ColumnMappings.Add("Current Index Date", "CurrentIndexDate");
            sqlBulkCopy.ColumnMappings.Add("End Index Date", "EndIndexDate");
            sqlBulkCopy.ColumnMappings.Add("Account Value", "AccountValue");
            sqlBulkCopy.ColumnMappings.Add("Raw Premium", "RawPremium");
            sqlBulkCopy.ColumnMappings.Add("Part Rate", "PartRate");
            sqlBulkCopy.ColumnMappings.Add("Spread Rate", "SpreadRate");
            sqlBulkCopy.ColumnMappings.Add("Cap Rate", "CapRate");
            sqlBulkCopy.ColumnMappings.Add("Floor Rate", "FloorRate");
            sqlBulkCopy.ColumnMappings.Add("Performance Trigger Rate", "PerformanceTriggerRate");
            sqlBulkCopy.ColumnMappings.Add("Beginning Index Value", "BeginningIndexValue");
            sqlBulkCopy.ColumnMappings.Add("Issue Date", "IssueDate");
            sqlBulkCopy.ColumnMappings.Add("Roll Indicator", "RollIndicator");
            sqlBulkCopy.ColumnMappings.Add("Fund Description", "FundDescription");
            sqlBulkCopy.ColumnMappings.Add("Product Name", "ProductName");
            sqlBulkCopy.ColumnMappings.Add("Coupon Rate", "CouponRate");
        }
        protected void FillDataTable(DataTable outputData, string conString, string source = null)
        {
            DateTime now = DateTime.UtcNow;
            using (TextReader reader = new StreamReader(conString))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    bool onFirstRow = true;
                    while (true)
                    {
                        string[] row = csv.Read();
                        if (onFirstRow)
                        {
                            onFirstRow = false;
                            continue;
                        }
                        if (row == null)
                        {
                            break;
                        }
                        DataRow toInsert = outputData.NewRow();
                        int i = 0;
                        foreach (DataColumn col in outputData.Columns)
                        {
                            if (col.ColumnName == "LoadDate")
                            {
                                toInsert["LoadDate"] = now;
                            }
                            else if (col.ColumnName == "Source")
                            {
                                toInsert["Source"] = source ?? AppDomain.CurrentDomain.FriendlyName;
                            }
                            else if (col.ColumnName == "UserId")
                            {
                                toInsert["UserId"] = Environment.UserName;
                            }
                            else if (col.ColumnName == "InforceDate")
                            {
                                continue;
                            }
                            else if (row[i] != "")
                            {
                                toInsert[col.ColumnName] = Convert.ChangeType(row[i++], col.DataType);
                            }
                            else
                            {
                                i++;
                            }
                            if (i == row.Count())
                            {
                                break;
                            }
                        }
                        outputData.Rows.Add(toInsert);
                    }
                }
            }
        }
    }
}
