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
using CsvHelper;
using Excel = Microsoft.Office.Interop.Excel;
using ExcelDataReader;
using System.Runtime.InteropServices;

namespace DataLoaderOptions
{
    class DataLoaderDLICNbFIA7YrS : DataLoader
    {

        string inputFolder;
        static object toLock = new object();
        int headerRow = 1;
        public DataLoaderDLICNbFIA7YrS()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA RS7\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC NB FIA RS7\\";
        }

        public override string SqlTableName => "DLIC.PoliciesNBFIAIssueState";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                DataTable outputData = CreateDataTable();
                DataTable secondSheet = outputData.Clone();
                string infile = OutputPath + Path.GetFileNameWithoutExtension(file) + ".csv";
                if (Path.GetExtension(file) != ".csv")
                {
                    ConvertExcelToCsvInterop(inputFolder + Path.GetFileName(file), inputFolder + Path.GetFileNameWithoutExtension(file) + ".csv");
                    File.Delete(file);
                }
                string fileCsv = inputFolder + Path.GetFileNameWithoutExtension(file) + ".csv";
                FillDataTable(outputData, GetConnString(fileCsv, true), infile);

                // Comment this out: this code is for reading an Excel file
                //Get data from the first sheet
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



                //CorrectDataTable(outputData);
                //outputData.AcceptChanges();

                //DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("Policy Issue Date")).Max();
                string outfile = infile;
                //DropColumns(outputData);

                if (outputData.Rows.Count > 0)
                {
                    //string filepath = OutputPath + Path.GetFileName(file);
                    //UpdateAsOfDate(outputData, asOfDate, infile);
                    //outputData.AcceptChanges();
                    lock (toLock)
                    {
                        string sqlString = ConfigurationManager.ConnectionStrings["Sql"].ConnectionString;
                        using (SqlConnection con = new SqlConnection(sqlString))
                        {
                            con.Open();
                            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                            {
                                //Set the database table name
                                sqlBulkCopy.DestinationTableName = SqlTableName;
                                MapTable(sqlBulkCopy);
                                sqlBulkCopy.BulkCopyTimeout = 0;
                                sqlBulkCopy.WriteToServer(outputData);
                            }
                            if (CheckSource())
                            {
                                LoadDLICNBFIA();
                            }
                            /*
                            if (ToLoad)
                            {
                                try
                                {
                                    using (SqlCommand cmd = new SqlCommand("DLIC.UpdateIssueState", con))
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
                            }*/
                        }
                    }

                    if (File.Exists(outfile))
                    {
                        File.Delete(outfile);
                    }
                    File.Move(fileCsv, outfile);
                }
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();


            tbl.Columns.Add("Contract Number", typeof(string));
            tbl.Columns.Add("Policy Status", typeof(string));
            tbl.Columns.Add("Policy Application Date", typeof(DateTime));
            tbl.Columns.Add("Policy Issue Date", typeof(DateTime));
            tbl.Columns.Add("Trade Date", typeof(DateTime));
            tbl.Columns.Add("Effective Date", typeof(DateTime));
            tbl.Columns.Add("System Date", typeof(DateTime));
            tbl.Columns.Add("Transaction Number", typeof(string));
            tbl.Columns.Add("Transaction Amount", typeof(string));
            tbl.Columns.Add("Transaction Type", typeof(string));
            tbl.Columns.Add("Issue State", typeof(string));
            tbl.Columns.Add("Qual Type", typeof(string));
            tbl.Columns.Add("Owner Name", typeof(string));
            tbl.Columns.Add("Owner Type", typeof(string));
            tbl.Columns.Add("Owner SSN", typeof(string));
            tbl.Columns.Add("Annuitant Name", typeof(string));
            tbl.Columns.Add("Annuitant Age", typeof(string));
            tbl.Columns.Add("Product", typeof(string));
            tbl.Columns.Add("Agent Number (Level 1)", typeof(string));
            tbl.Columns.Add("Agent Name (Level 1)", typeof(string));
            tbl.Columns.Add("Agent Percentage", typeof(string));
            tbl.Columns.Add("Agent Phone Number", typeof(string));
            tbl.Columns.Add("Agent Email Address", typeof(string));
            tbl.Columns.Add("Agent Number (Level 2)", typeof(string));
            tbl.Columns.Add("Level 2 (Level 1 Next Higher)", typeof(string));
            tbl.Columns.Add("Level 2 Type", typeof(string));
            tbl.Columns.Add("Agent Number (Level 3)", typeof(string));
            tbl.Columns.Add("Level 3 (Level 2 Next Higher)", typeof(string));
            tbl.Columns.Add("Level 3 Type", typeof(string));
            tbl.Columns.Add("Agent Number (Level 4)", typeof(string));
            tbl.Columns.Add("Level 4 (Level 3 Next Higher)", typeof(string));
            tbl.Columns.Add("Level 4 Type", typeof(string));
            tbl.Columns.Add("IMO/BD/Bank", typeof(string));
            tbl.Columns.Add("Source of Funds", typeof(string));
            tbl.Columns.Add("Previous Contract Number", typeof(string));
            tbl.Columns.Add("Gross Pending", typeof(string));
            tbl.Columns.Add("Estimated Amount", typeof(string));
            tbl.Columns.Add("Exchange Money Received", typeof(string));
            tbl.Columns.Add("Exchange Type", typeof(string));
            tbl.Columns.Add("Exchange Date Received", typeof(string));
            tbl.Columns.Add("Transfer Sign Date", typeof(DateTime));
            tbl.Columns.Add("Rate Lock End Date", typeof(DateTime));
            tbl.Columns.Add("Terminated / Surr Date", typeof(DateTime));
            tbl.Columns.Add("Replacement", typeof(string));
            tbl.Columns.Add("Transfer Out Multiple", typeof(string));
            tbl.Columns.Add("Surrender Period ", typeof(string));
            tbl.Columns.Add("E-App Indicator", typeof(string));
            tbl.Columns.Add("GLWB Election", typeof(string));
            tbl.Columns.Add("Agent Commission", typeof(string));
            tbl.Columns.Add("INCOME RIDER ELECTION", typeof(string));
            tbl.Columns.Add("ROP", typeof(string));
            tbl.Columns.Add("Single/Multiple Allocation", typeof(string));
            tbl.Columns.Add("Source").DefaultValue = "RS 7";

            return tbl;
        }
        //protected void CorrectDataTable(DataTable outputData)
        //{
        //    int i = 0;

        //    foreach (DataRow row in outputData.AsEnumerable().ToList())
        //    {

        //        if (row["Policy"] == DBNull.Value)
        //        {
        //            row.Delete();
        //        }
        //        else
        //        {
        //            row["Issue Date"] = DateTime.ParseExact(row["Policy Issue Date"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture).ToString("yyyy-MM-dd");
        //            //string fundType;
        //            //if (FundType.ContainsKey((string)row["Fund Description"]))
        //            //{
        //            //    fundType = FundType[(string)row["Fund Description"]];
        //            //}
        //            //else
        //            //{
        //            //    fundType = "";
        //            //}
        //            //if (!string.IsNullOrEmpty(fundType))
        //            //{
        //            //    decimal value = (decimal)row[fundType];
        //            //    if (value == 0)
        //            //    {
        //            //        if ((decimal)row["Floor Rate"] != 0)
        //            //        {
        //            //            row[fundType] = (decimal)row["Floor Rate"];
        //            //        }
        //            //    }
        //            //    else if (value > 10 && !fundType.Equals("Part Rate", StringComparison.InvariantCultureIgnoreCase))
        //            //    {
        //            //        while (value > 10)
        //            //        {
        //            //            value /= 10;
        //            //        }
        //            //        row[fundType] = value;
        //            //    }
        //            //    else if (value > 200 && fundType.Equals("Part Rate", StringComparison.InvariantCultureIgnoreCase))
        //            //    {
        //            //        while (value > 200)
        //            //        {
        //            //            value /= 10;
        //            //        }
        //            //        row[fundType] = value;
        //            //    }
        //            //    row["Floor Rate"] = 0;
        //            //}
        //            i++;
        //        }
        //    }
        //}

        //protected void DropColumns(DataTable outputData)
        //{
        //    string[] ColumnArray = { "Contract Number", "Issue State" };

        //    foreach ( DataColumn col in outputData.Columns)
        //    {
        //        int pos = Array.IndexOf(ColumnArray, col.ColumnName);
        //        if (pos<=-1)
        //        { outputData.Columns.Remove(col.ColumnName); }
        //    }
        //}
        protected void UpdateAsOfDate(DataTable outputData, DateTime asOfDate, string source)
        {
            // outputData.Columns.CanRemove("LoadDate", typeof(DateTime));
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

            sqlBulkCopy.ColumnMappings.Add("Contract Number", "ContractNumber");
            sqlBulkCopy.ColumnMappings.Add("Issue State", "IssueState");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
        }

        static void ConvertExcelToCsvInterop(string excelFilePath, string csvOutputFile, int worksheetNumber = 1)
        {
            if (File.Exists(csvOutputFile))
            {
                //throw new ArgumentException("File exists: " + csvOutputFile);
                File.Delete(csvOutputFile);
            }

            var myExcelApp = new Excel.Application()
            {
                Visible = false
            };

            Excel.Workbook myExcelWorkbooks = myExcelApp.Workbooks.Open(Filename: excelFilePath, ReadOnly: false);

            //myExcelWorkbooks.SaveAs(csvOutputFile);
            myExcelWorkbooks.SaveAs(csvOutputFile, Microsoft.Office.Interop.Excel.XlFileFormat.xlCSVWindows);

            myExcelWorkbooks.Close();

            myExcelApp.Quit();

            Marshal.ReleaseComObject(myExcelApp);
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
                            //if (col.ColumnName == "LoadDate")
                            //{
                            //    toInsert["LoadDate"] = now;
                            //}
                            //else if (col.ColumnName == "Source")
                            //{
                            //    toInsert["Source"] = source ?? AppDomain.CurrentDomain.FriendlyName;
                            //}
                            //else if (col.ColumnName == "UserId")
                            //{
                            //    toInsert["UserId"] = Environment.UserName;
                            //}
                            //else if (col.ColumnName == "InforceDate")
                            //{
                            //    continue;
                            //}
                            //else 
                            if (row[0] != "")
                            {
                                if (col.ColumnName == "Contract Number" || col.ColumnName == "Issue State")
                                {
                                    toInsert[col.ColumnName] = Convert.ChangeType(row[i++], col.DataType);
                                }
                                else
                                {
                                    i++;
                                }

                            }


                            //else if (row[i] != "")
                            //{
                            //    try
                            //    {
                            //        toInsert[col.ColumnName] = Convert.ChangeType(row[i++], col.DataType);
                            //    }
                            //    catch ( FormatException fex)
                            //    {
                            //        if (fex.Message == "String was not recognized as a valid DateTime.")
                            //        {
                            //            toInsert[col.ColumnName] = Convert.ChangeType("12 /1/9999", col.DataType);
                            //        }

                            //    }

                            //}
                            //else
                            //{
                            //   ;
                            //}
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
