using CsvHelper;
using Extensions;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using log4net;


namespace DataLoaderOptions.Loaders
{
    class DataLoaderGLAC : DataLoader
    {
        private static readonly ILog log = LogManager.GetLogger(Environment.MachineName);
        string _folderPath;
        bool _hasHeaders;
        private List<IFixedWidthField> _fields = new List<IFixedWidthField>();

        public override string SqlTableName => "GIS.PoliciesGLAC";

        public DataLoaderGLAC()
        {
            _folderPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\GLAC\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\GLAC\\";
            _hasHeaders = false;
        }

        /// <summary>
        /// Scan the input directory for GLAC hedge CSV files, and load each of them to
        /// GIS.PoliciesGLAC_test
        /// </summary>
        public override void LoadToSql()
        {
            // Scan input directory for files
            string[] files = Directory.GetFiles(_folderPath, "*", System.IO.SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                // Create empty DataTable with required columns
                DataTable outputData = CreateDataTable();

                // Fill the DataTable with data
                string fileName = OutputPath + Path.GetFileName(file);
                var conString = GetConnString(file, true);
                FillDataTable(outputData, conString, fileName);
                outputData.AcceptChanges();

                // Add load details to the data
                DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("Effective-Date")).Max();
                string outfile = OutputPath + "GLAC_HEDGING_OPTIONS_" + asOfDate.ToString("yyyyMMdd") + ".csv";
                AddLoadDetails(outputData, outfile);

                // Load the data to the file-specific staging table and run the HedgeFile generator
                base.LoadData(outputData);
                OutputHedgeFile();

                // Move the data from the file-specific staging table to the general staging table
                if (ToLoad)
                {
                    string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
                    using (SqlConnection con = new SqlConnection(sqlString))
                    {
                        con.Open();
                        try
                        {
                            using (SqlCommand cmd = new SqlCommand("GIS.InsertGLAC", con))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.CommandTimeout = 0;
                                cmd.ExecuteNonQuery();
                            }
                        }
                        catch (Exception ex)
                        {
                            log.Fatal("Error with DataLoader GLAC" + asOfDate);
                            Console.WriteLine(ex.Message);
                        }
                    }
                }

                // Move the input file to the parent directory, deleting a synonymous file if it exists
                if (File.Exists(outfile))
                {
                    File.Delete(outfile);
                }
                // Don't move the file while we're testing.
                File.Move(file, outfile);
            }
        }


        public void OutputHedgeFile()
        {
            string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;

            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();

                // Get the effective date from the hedge file
                DateTime asOfDate;
                using (SqlCommand cmd = new SqlCommand("SELECT MAX(EffectiveDate) FROM GIS.PoliciesGLAC", con))
                {
                    asOfDate = (DateTime)cmd.ExecuteScalar();
                }

                // Generate the hedge file with HedgeRatio added, and then output the data
                DataTable dt = new DataTable();
                using (SqlCommand cmd = new SqlCommand("EXEC GIS.GenerateHedgeFile", con))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        adapter.Fill(dt);
                    }
                    var path = @"\\sv351018\nas6\actuary\DACT\ALM\FIAHedging\HedgeRatios\HedgeRatio output files for Steve Wilson\GLAC_hedge_withHedgeRatio_" + asOfDate.ToString("yyyyMMdd") + ".csv";
                    dt.ToCSV(path, onlyDatePart: true);
                    Console.WriteLine("CSV file output -- original");
                }
            }
        }

        /// <summary>
        /// Create an empty DataTable with the required columns and data types for
        /// the new GLAC hedge file.
        /// </summary>
        /// <returns>An empty DataTable.</returns>
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();

            tbl.Columns.Add("Effective-Date", typeof(DateTime));
            tbl.Columns.Add("Source", typeof(string));
            tbl.Columns.Add("Company-Code", typeof(string));
            tbl.Columns.Add("Balance-Sheet", typeof(string));
            tbl.Columns.Add("Strategy", typeof(string));
            tbl.Columns.Add("Contract-Number", typeof(string));
            tbl.Columns.Add("Issue-Age", typeof(int));
            tbl.Columns.Add("Issue-Date", typeof(DateTime));
            tbl.Columns.Add("Renewal-Date", typeof(DateTime));
            tbl.Columns.Add("End-of-SC-Period", typeof(DateTime));
            tbl.Columns.Add("Fund-in-Strategy", typeof(decimal));
            tbl.Columns.Add("Type", typeof(string));
            tbl.Columns.Add("Index-Description", typeof(string));
            tbl.Columns.Add("Frequency-Magnitude", typeof(int));
            tbl.Columns.Add("Frequency-Unit", typeof(string));
            tbl.Columns.Add("Reset-Magnitude", typeof(int));
            tbl.Columns.Add("Reset-Unit", typeof(string));
            tbl.Columns.Add("Type-Cap", typeof(int));
            tbl.Columns.Add("Type-Part-Rate", typeof(int));
            tbl.Columns.Add("Type-Spread", typeof(int));
            tbl.Columns.Add("Type-Floor", typeof(int));
            tbl.Columns.Add("Type-IBR", typeof(int));
            tbl.Columns.Add("Type-IBR-Rollup", typeof(int));
            tbl.Columns.Add("Index-Cap", typeof(decimal));
            tbl.Columns.Add("Index-Part-Rate", typeof(decimal));
            tbl.Columns.Add("Index-Spread", typeof(decimal));
            tbl.Columns.Add("Index-Floor", typeof(string));
            tbl.Columns.Add("Reserve-to-CSV-Floor", typeof(string));
            tbl.Columns.Add("Index-#", typeof(decimal));
            tbl.Columns.Add("ARCVAL-Fund-Code", typeof(string));

            return tbl;
        }


        /// <summary>
        /// Maps the DataTable columns to the SQL table columns.
        /// </summary>
        /// <param name="sqlBulkCopy">The SQL bulk copy object.</param>
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("Effective-Date", "EffectiveDate");
            sqlBulkCopy.ColumnMappings.Add("Company-Code", "CompanyCode");
            sqlBulkCopy.ColumnMappings.Add("Balance-Sheet", "BalanceSheet");
            sqlBulkCopy.ColumnMappings.Add("Strategy", "Strategy");
            sqlBulkCopy.ColumnMappings.Add("Contract-Number", "ContractNumber");
            sqlBulkCopy.ColumnMappings.Add("Issue-Age", "IssueAge");
            sqlBulkCopy.ColumnMappings.Add("Issue-Date", "IssueDate");
            sqlBulkCopy.ColumnMappings.Add("Renewal-Date", "RenewalDate");
            sqlBulkCopy.ColumnMappings.Add("End-of-SC-Period", "EndofSCPeriod");
            sqlBulkCopy.ColumnMappings.Add("Fund-in-Strategy", "FundinStrategy");
            sqlBulkCopy.ColumnMappings.Add("Type", "Type");
            sqlBulkCopy.ColumnMappings.Add("Index-Description", "IndexDescription");
            sqlBulkCopy.ColumnMappings.Add("Frequency-Magnitude", "FrequencyMagnitude");
            sqlBulkCopy.ColumnMappings.Add("Frequency-Unit", "FrequencyUnit");
            sqlBulkCopy.ColumnMappings.Add("Reset-Magnitude", "ResetMagnitude");
            sqlBulkCopy.ColumnMappings.Add("Reset-Unit", "ResetUnit");
            sqlBulkCopy.ColumnMappings.Add("Type-Cap", "TypeCap");
            sqlBulkCopy.ColumnMappings.Add("Type-Part-Rate", "TypePartRate");
            sqlBulkCopy.ColumnMappings.Add("Type-Spread", "TypeSpread");
            sqlBulkCopy.ColumnMappings.Add("Type-Floor", "TypeFloor");
            sqlBulkCopy.ColumnMappings.Add("Type-IBR", "TypeIBR");
            sqlBulkCopy.ColumnMappings.Add("Type-IBR-Rollup", "TypeIBRRollup");
            sqlBulkCopy.ColumnMappings.Add("Index-Cap", "IndexCap");
            sqlBulkCopy.ColumnMappings.Add("Index-Part-Rate", "IndexPartRate");
            sqlBulkCopy.ColumnMappings.Add("Index-Spread", "IndexSpread");
            sqlBulkCopy.ColumnMappings.Add("Index-Floor", "IndexFloor");
            sqlBulkCopy.ColumnMappings.Add("Reserve-to-CSV-Floor", "ReservetoCSVFloor");
            sqlBulkCopy.ColumnMappings.Add("Index-#", "IndexNumber");
            sqlBulkCopy.ColumnMappings.Add("ARCVAL-Fund-Code", "ARCVALFundCode");
        }


        /// <summary>
        /// Add the load date, file source, and username to the output DataTable.
        /// </summary>
        /// <param name="outputData">The DataTable that contains the GLAC hedge data</param>
        /// <param name="source">The filepath.</param>
        protected void AddLoadDetails(DataTable outputData, string source)
        {
            outputData.Columns.Add("LoadDate", typeof(DateTime));
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


        /// <summary>
        /// Fill an empty DataTable with data from the GLAC hedge CSV file. Note that the
        /// DataTable must have the columns and data types pre-initialized.
        /// </summary>
        /// <param name="outputData">Empty DataTable.</param>
        /// <param name="conString">Connection string to our hedging database.</param>
        /// <param name="source">File path.</param>
        protected void FillDataTable(DataTable outputData, string conString, string source = null)
        {
            DateTime now = DateTime.UtcNow;
            using (TextReader reader = new StreamReader(conString))
            {
                using (CsvHelper.ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    int rowNumber = 0;
                    while (true)
                    {
                        string[] row = csv.Read();
                        if (row == null)
                        {
                            break;
                        }

                        if (rowNumber++ == 0)
                        {
                            continue;
                        }

                        DataRow toInsert = outputData.NewRow();

                        int i = 0;
                        foreach (DataColumn col in outputData.Columns)
                        {
                            if (row[i] != "")
                            {
                                var val = row[i++];
                                toInsert[col.ColumnName] = Convert.ChangeType(val, col.DataType);
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
