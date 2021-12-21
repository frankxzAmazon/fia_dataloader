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
    class DataLoaderNAC : DataLoader
    {
        string inputFolder;
        static object toLock = new object();
        string wsName = "Triton";
        int headerRow = 7;
        public DataLoaderNAC()
        {
            inputFolder = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\NAC\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\NAC\\";
        }

        public override string SqlTableName => "GIS.PoliciesNAC";
        public override void LoadToSql()
        {

            string[] files = Directory.GetFiles(inputFolder.ToString(), "*FIAs for GLAC*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                string fileName = OutputPath + Path.GetFileName(file);
                if (!File.Exists(fileName))
                {

                    string filepath = OutputPath + Path.GetFileName(file);
                    DataTable outputData = CreateDataTable();

                    DataReader_Excel xl = new DataReader_Excel(file, wsName, headerRow);
                    xl.SetDataTableFormat(outputData);
                    outputData = xl.GetFilledDataTable(OnError.UseNullValue,true,null,true);
                    DateTime asOfDate = (DateTime)xl.GetCell(3, 1);

                    UpdateAsOfDate(outputData, asOfDate, fileName);
                    CorrectDataTable(outputData);
                    outputData.AcceptChanges();
                    lock (toLock)
                    {
                        base.LoadData(outputData);
                        if (ToLoad)
                        {
                            string sqlString = ConfigurationManager.ConnectionStrings["Sql"].ConnectionString;
                            using (SqlConnection con = new SqlConnection(sqlString))
                            {
                                con.Open();
                                try
                                {
                                    using (SqlCommand cmd = new SqlCommand("GIS.InsertNAC", con))
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
                    File.Move(file, fileName);
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
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("InforceDate", typeof(DateTime)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("CONTRACTNO", typeof(string)) { AllowDBNull = false });
            tbl.Columns.Add(new DataColumn("VERSION", typeof(string)) { AllowDBNull = false });
            tbl.Columns.Add(new DataColumn("Co", typeof(int)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("GpInd", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("MVAInd", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("PLANID", typeof(string)) { AllowDBNull = false });
            tbl.Columns.Add(new DataColumn("GLAC", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("PLink_Plan", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("IDATE", typeof(DateTime)) { AllowDBNull = false });
            tbl.Columns.Add(new DataColumn("SumOfFV2", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfCV0", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfSTATTOTAL", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfTAXTOTAL", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("Fixed FV", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("Ave FV", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("PtP FV", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("MPtP FV", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("EICAP", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("EICAPM", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("GMWBSIG", typeof(int)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("GMWB_FSCH", typeof(int)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("GMWB_BCRG", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("GMWB_RCRG", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfGMWB_BASE0", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfGMWB_BEN0", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("FirstOfSTATIRATE", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("FirstOfVMORTTABLE", typeof(int)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("SumOfFVMIN0", typeof(decimal)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("Count", typeof(int)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("Index", typeof(string)) { AllowDBNull = true });
            tbl.Columns.Add(new DataColumn("EISPREAD", typeof(string)) { AllowDBNull = true });

            return tbl;
        }
        protected void CorrectDataTable(DataTable outputData)
        {
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("CONTRACTNO", "CONTRACTNO");
            sqlBulkCopy.ColumnMappings.Add("VERSION", "VERSION");
            sqlBulkCopy.ColumnMappings.Add("Co", "Co");
            sqlBulkCopy.ColumnMappings.Add("GpInd", "GpInd");
            sqlBulkCopy.ColumnMappings.Add("MVAInd", "MVAInd");
            sqlBulkCopy.ColumnMappings.Add("PLANID", "PLANID");
            sqlBulkCopy.ColumnMappings.Add("GLAC", "GLAC");
            sqlBulkCopy.ColumnMappings.Add("PLink_Plan", "PLink_Plan");
            sqlBulkCopy.ColumnMappings.Add("IDATE", "IDATE");
            sqlBulkCopy.ColumnMappings.Add("SumOfFV2", "SumOfFV2");
            sqlBulkCopy.ColumnMappings.Add("SumOfCV0", "SumOfCV0");
            sqlBulkCopy.ColumnMappings.Add("SumOfSTATTOTAL", "SumOfSTATTOTAL");
            sqlBulkCopy.ColumnMappings.Add("SumOfTAXTOTAL", "SumOfTAXTOTAL");
            sqlBulkCopy.ColumnMappings.Add("Fixed FV", "Fixed FV");
            sqlBulkCopy.ColumnMappings.Add("Ave FV", "Ave FV");
            sqlBulkCopy.ColumnMappings.Add("PtP FV", "PtP FV");
            sqlBulkCopy.ColumnMappings.Add("MPtP FV", "MPtP FV");
            sqlBulkCopy.ColumnMappings.Add("EICAP", "EICAP");
            sqlBulkCopy.ColumnMappings.Add("EICAPM", "EICAPM");
            sqlBulkCopy.ColumnMappings.Add("GMWBSIG", "GMWBSIG");
            sqlBulkCopy.ColumnMappings.Add("GMWB_FSCH", "GMWB_FSCH");
            sqlBulkCopy.ColumnMappings.Add("GMWB_BCRG", "GMWB_BCRG");
            sqlBulkCopy.ColumnMappings.Add("GMWB_RCRG", "GMWB_RCRG");
            sqlBulkCopy.ColumnMappings.Add("SumOfGMWB_BASE0", "SumOfGMWB_BASE0");
            sqlBulkCopy.ColumnMappings.Add("SumOfGMWB_BEN0", "SumOfGMWB_BEN0");
            sqlBulkCopy.ColumnMappings.Add("FirstOfSTATIRATE", "FirstOfSTATIRATE");
            sqlBulkCopy.ColumnMappings.Add("FirstOfVMORTTABLE", "FirstOfVMORTTABLE");
            sqlBulkCopy.ColumnMappings.Add("SumOfFVMIN0", "SumOfFVMIN0");
            sqlBulkCopy.ColumnMappings.Add("Count", "Count");
            sqlBulkCopy.ColumnMappings.Add("Index", "Index");
            sqlBulkCopy.ColumnMappings.Add("EISPREAD", "EISPREAD");

        }
    }
}
