using CsvHelper;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
namespace DataLoaderOptions
{
    class DataLoaderSundex : DataLoader
    {
        string _folderPath;
        string _zipstart;
        bool _hasHeaders;
        static object toLock = new object();
        public DataLoaderSundex()
        {
            _folderPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC Sundex\\ToUpload\\";
            OutputPath = "W:\\DACT\\ALM\\FIAHedging\\DBUpload\\DLIC Sundex\\";
            _hasHeaders = true;
        }
        public override string SqlTableName => "DLIC.PoliciesSundex";
        public override void LoadToSql()
        {
            string[] files = Directory.GetFiles(_folderPath, "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                string outputPath = OutputPath + Path.GetFileName(file);
                if (Path.GetExtension(file).ToLowerInvariant() == ".txt")
                {
                    if (File.Exists(outputPath))
                    {
                        File.Delete(outputPath);
                    }
                    File.Move(file, outputPath);
                    Upload(outputPath, SqlTableName, _hasHeaders);
                }
            }
            //}
        }
        private void Upload(string filePath, string sqlTable, bool hasHeader)
        {
            string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
            DataTable outputData = CreateDataTable();
            FillDataTable(outputData, GetConnString(filePath, hasHeader));
            DateTime asOfDate = outputData.Rows[2].Field<DateTime>("LMPDate");
            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();

                using (SqlCommand cmd = new SqlCommand($"delete from {SqlTableName}", con))
                {
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("delete from OptionInventoryStagingTable", con))
                {
                    //   cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                }
                Int32 count;
                using (SqlCommand sqlCommand = new SqlCommand("select count(*) from " + sqlTable + " where LMPDate like '" + asOfDate.ToShortDateString() + "'", con))
                {
                    count = (Int32)sqlCommand.ExecuteScalar();
                }
                if (count == 0)
                {
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {
                        //Set the database table name
                        sqlBulkCopy.DestinationTableName = sqlTable;
                        MapTable(sqlBulkCopy);
                        AddLoadDetails(outputData, filePath);
                        sqlBulkCopy.WriteToServer(outputData);
                    }
                }
                if (ToLoad)
                {
                    try
                    {
                        using (SqlCommand cmd = new SqlCommand("DLIC.InsertSundex", con))
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
                con.Close();
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("PLANID", typeof(string));
            tbl.Columns.Add("VERSION", typeof(string));
            tbl.Columns.Add("COHORT", typeof(string));
            tbl.Columns.Add("SUBCOHORT", typeof(string));
            tbl.Columns.Add("CONTRACTNO", typeof(string));
            tbl.Columns.Add("GROUPNO", typeof(long));
            tbl.Columns.Add("SEGMENT", typeof(long));
            tbl.Columns.Add("COMPANY", typeof(string));
            tbl.Columns.Add("STATE", typeof(string));
            tbl.Columns.Add("QUALIFIED", typeof(string));
            tbl.Columns.Add("MARKET", typeof(string));
            tbl.Columns.Add("AGENCY", typeof(string));
            tbl.Columns.Add("STATUS", typeof(string));
            tbl.Columns.Add("IDATE", typeof(DateTime));
            tbl.Columns.Add("OSDATE", typeof(DateTime));
            tbl.Columns.Add("LMPDate", typeof(DateTime));
            tbl.Columns.Add("NADATE", typeof(DateTime));
            tbl.Columns.Add("EDATE", typeof(DateTime));
            tbl.Columns.Add("RODATE", typeof(DateTime));
            tbl.Columns.Add("MDATE", typeof(DateTime));
            tbl.Columns.Add("TDATE", typeof(DateTime));
            tbl.Columns.Add("RDATE", typeof(DateTime));
            tbl.Columns.Add("X", typeof(long));
            tbl.Columns.Add("Y", typeof(long));
            tbl.Columns.Add("XA", typeof(long));
            tbl.Columns.Add("YA", typeof(long));
            tbl.Columns.Add("SEXX", typeof(string));
            tbl.Columns.Add("SEXY", typeof(string));
            tbl.Columns.Add("SEXXA", typeof(string));
            tbl.Columns.Add("SEXYA", typeof(string));
            tbl.Columns.Add("GCANN0", typeof(decimal));
            tbl.Columns.Add("MODE", typeof(string));
            tbl.Columns.Add("FLEXIBLE", typeof(string));
            tbl.Columns.Add("MPY", typeof(long));
            tbl.Columns.Add("BXIND", typeof(string));
            tbl.Columns.Add("BDFACE", typeof(decimal));
            tbl.Columns.Add("BDFACP", typeof(decimal));
            tbl.Columns.Add("FV1_PRIOR", typeof(decimal));
            tbl.Columns.Add("FV2_PRIOR", typeof(decimal));
            tbl.Columns.Add("FV2", typeof(decimal));
            tbl.Columns.Add("CV0", typeof(decimal));
            tbl.Columns.Add("LV0", typeof(decimal));
            tbl.Columns.Add("GIDATE", typeof(DateTime));
            tbl.Columns.Add("GIRATE0", typeof(decimal));
            tbl.Columns.Add("GIRATE1", typeof(decimal));
            tbl.Columns.Add("GIRATEU", typeof(decimal));
            tbl.Columns.Add("IBAILOUT0", typeof(decimal));
            tbl.Columns.Add("CGC0", typeof(decimal));
            tbl.Columns.Add("CFV0", typeof(decimal));
            tbl.Columns.Add("SUMGC0", typeof(decimal));
            tbl.Columns.Add("SUMPW0", typeof(decimal));
            tbl.Columns.Add("RS_NH", typeof(string));
            tbl.Columns.Add("RS_NE1", typeof(string));
            tbl.Columns.Add("YTDGCF", typeof(decimal));
            tbl.Columns.Add("YTDGCR", typeof(decimal));
            tbl.Columns.Add("YTDPWF", typeof(decimal));
            tbl.Columns.Add("YTDPWNF", typeof(decimal));
            tbl.Columns.Add("YTDXFV", typeof(decimal));
            tbl.Columns.Add("BDOPT", typeof(int));
            tbl.Columns.Add("RABD0", typeof(decimal));
            tbl.Columns.Add("REBD0", typeof(decimal));
            tbl.Columns.Add("RUBD0", typeof(decimal));
            tbl.Columns.Add("MVA0", typeof(decimal));
            tbl.Columns.Add("XIMECHARGE", typeof(decimal));
            tbl.Columns.Add("GMWBEDATE", typeof(DateTime));
            tbl.Columns.Add("GMWBSIG", typeof(string));
            tbl.Columns.Add("AG4MCGMWB0", typeof(string));
            tbl.Columns.Add("GMABEDATE", typeof(DateTime));
            tbl.Columns.Add("GMABSIG", typeof(string));
            tbl.Columns.Add("AG4MCGMAB0", typeof(string));
            tbl.Columns.Add("GMAWBAMNT", typeof(string));
            tbl.Columns.Add("GMAWBMA", typeof(string));
            tbl.Columns.Add("GMAWBRA", typeof(string));
            tbl.Columns.Add("ECPWAIVE", typeof(string));
            tbl.Columns.Add("ISTATE", typeof(string));
            tbl.Columns.Add("GROUPID", typeof(string));
            tbl.Columns.Add("ACCTTYPE", typeof(string));
            tbl.Columns.Add("GALSEG", typeof(string));
            tbl.Columns.Add("ADMNSTAT", typeof(string));
            tbl.Columns.Add("INACTRANS", typeof(string));
            tbl.Columns.Add("SCWAIVE", typeof(string));
            tbl.Columns.Add("AGENT", typeof(string));
            tbl.Columns.Add("VARIANT", typeof(string));
            tbl.Columns.Add("GCOMMID", typeof(string));
            tbl.Columns.Add("SLCRE", typeof(string));
            tbl.Columns.Add("EITERMDATE", typeof(DateTime));
            tbl.Columns.Add("EITERM", typeof(long));
            tbl.Columns.Add("EIPRATE", typeof(decimal));
            tbl.Columns.Add("EICAP", typeof(decimal));
            tbl.Columns.Add("EIOV0", typeof(decimal));
            tbl.Columns.Add("EIPRATEM", typeof(decimal));
            tbl.Columns.Add("EI_INDEX0", typeof(decimal));
            tbl.Columns.Add("EI_INDEXHW", typeof(decimal));
            tbl.Columns.Add("EIOV_BOT", typeof(decimal));
            tbl.Columns.Add("EIOV_PA", typeof(decimal));
            tbl.Columns.Add("EIOV_VD", typeof(decimal));
            tbl.Columns.Add("EIFVMINBOT", typeof(decimal));
            tbl.Columns.Add("EIFVBOT", typeof(decimal));
            tbl.Columns.Add("EIPRATE2", typeof(decimal));
            tbl.Columns.Add("EIPRLMT", typeof(decimal));
            tbl.Columns.Add("KP_REMPREM", typeof(decimal));
            tbl.Columns.Add("KP_SPCRED", typeof(decimal));
            tbl.Columns.Add("ICMIN2", typeof(decimal));
            tbl.Columns.Add("FVMIN2LD", typeof(decimal));
            tbl.Columns.Add("EI_VAL_LST_ANNI", typeof(decimal));
            tbl.Columns.Add("CARVM_OVRT", typeof(long));
            tbl.Columns.Add("DLCOHORT", typeof(string));
            tbl.Columns.Add("RECORDIDA", typeof(string));

            return tbl;
        }
        protected DataTable CreateDataTable(DateTime date)
        {
            return CreateDataTable();
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
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("PLANID", "PLANID");
            sqlBulkCopy.ColumnMappings.Add("VERSION", "VERSION");
            sqlBulkCopy.ColumnMappings.Add("COHORT", "COHORT");
            sqlBulkCopy.ColumnMappings.Add("SUBCOHORT", "SUBCOHORT");
            sqlBulkCopy.ColumnMappings.Add("CONTRACTNO", "CONTRACTNO");
            sqlBulkCopy.ColumnMappings.Add("GROUPNO", "GROUPNO");
            sqlBulkCopy.ColumnMappings.Add("SEGMENT", "SEGMENT");
            sqlBulkCopy.ColumnMappings.Add("COMPANY", "COMPANY");
            sqlBulkCopy.ColumnMappings.Add("STATE", "STATE");
            sqlBulkCopy.ColumnMappings.Add("QUALIFIED", "QUALIFIED");
            sqlBulkCopy.ColumnMappings.Add("MARKET", "MARKET");
            sqlBulkCopy.ColumnMappings.Add("AGENCY", "AGENCY");
            sqlBulkCopy.ColumnMappings.Add("STATUS", "STATUS");
            sqlBulkCopy.ColumnMappings.Add("IDATE", "IDATE");
            sqlBulkCopy.ColumnMappings.Add("OSDATE", "OSDATE");
            sqlBulkCopy.ColumnMappings.Add("LMPDate", "LMPDate");
            sqlBulkCopy.ColumnMappings.Add("NADATE", "NADATE");
            sqlBulkCopy.ColumnMappings.Add("EDATE", "EDATE");
            sqlBulkCopy.ColumnMappings.Add("RODATE", "RODATE");
            sqlBulkCopy.ColumnMappings.Add("MDATE", "MDATE");
            sqlBulkCopy.ColumnMappings.Add("TDATE", "TDATE");
            sqlBulkCopy.ColumnMappings.Add("RDATE", "RDATE");
            sqlBulkCopy.ColumnMappings.Add("X", "X");
            sqlBulkCopy.ColumnMappings.Add("Y", "Y");
            sqlBulkCopy.ColumnMappings.Add("XA", "XA");
            sqlBulkCopy.ColumnMappings.Add("YA", "YA");
            sqlBulkCopy.ColumnMappings.Add("SEXX", "SEXX");
            sqlBulkCopy.ColumnMappings.Add("SEXY", "SEXY");
            sqlBulkCopy.ColumnMappings.Add("SEXXA", "SEXXA");
            sqlBulkCopy.ColumnMappings.Add("SEXYA", "SEXYA");
            sqlBulkCopy.ColumnMappings.Add("GCANN0", "GCANN0");
            sqlBulkCopy.ColumnMappings.Add("MODE", "MODE");
            sqlBulkCopy.ColumnMappings.Add("FLEXIBLE", "FLEXIBLE");
            sqlBulkCopy.ColumnMappings.Add("MPY", "MPY");
            sqlBulkCopy.ColumnMappings.Add("BXIND", "BXIND");
            sqlBulkCopy.ColumnMappings.Add("BDFACE", "BDFACE");
            sqlBulkCopy.ColumnMappings.Add("BDFACP", "BDFACP");
            sqlBulkCopy.ColumnMappings.Add("FV1_PRIOR", "FV1_PRIOR");
            sqlBulkCopy.ColumnMappings.Add("FV2_PRIOR", "FV2_PRIOR");
            sqlBulkCopy.ColumnMappings.Add("FV2", "FV2");
            sqlBulkCopy.ColumnMappings.Add("CV0", "CV0");
            sqlBulkCopy.ColumnMappings.Add("LV0", "LV0");
            sqlBulkCopy.ColumnMappings.Add("GIDATE", "GIDATE");
            sqlBulkCopy.ColumnMappings.Add("GIRATE0", "GIRATE0");
            sqlBulkCopy.ColumnMappings.Add("GIRATE1", "GIRATE1");
            sqlBulkCopy.ColumnMappings.Add("GIRATEU", "GIRATEU");
            sqlBulkCopy.ColumnMappings.Add("IBAILOUT0", "IBAILOUT0");
            sqlBulkCopy.ColumnMappings.Add("CGC0", "CGC0");
            sqlBulkCopy.ColumnMappings.Add("CFV0", "CFV0");
            sqlBulkCopy.ColumnMappings.Add("SUMGC0", "SUMGC0");
            sqlBulkCopy.ColumnMappings.Add("SUMPW0", "SUMPW0");
            sqlBulkCopy.ColumnMappings.Add("RS_NH", "RS_NH");
            sqlBulkCopy.ColumnMappings.Add("RS_NE1", "RS_NE1");
            sqlBulkCopy.ColumnMappings.Add("YTDGCF", "YTDGCF");
            sqlBulkCopy.ColumnMappings.Add("YTDGCR", "YTDGCR");
            sqlBulkCopy.ColumnMappings.Add("YTDPWF", "YTDPWF");
            sqlBulkCopy.ColumnMappings.Add("YTDPWNF", "YTDPWNF");
            sqlBulkCopy.ColumnMappings.Add("YTDXFV", "YTDXFV");
            sqlBulkCopy.ColumnMappings.Add("BDOPT", "BDOPT");
            sqlBulkCopy.ColumnMappings.Add("RABD0", "RABD0");
            sqlBulkCopy.ColumnMappings.Add("REBD0", "REBD0");
            sqlBulkCopy.ColumnMappings.Add("RUBD0", "RUBD0");
            sqlBulkCopy.ColumnMappings.Add("MVA0", "MVA0");
            sqlBulkCopy.ColumnMappings.Add("XIMECHARGE", "XIMECHARGE");
            sqlBulkCopy.ColumnMappings.Add("GMWBEDATE", "GMWBEDATE");
            sqlBulkCopy.ColumnMappings.Add("GMWBSIG", "GMWBSIG");
            sqlBulkCopy.ColumnMappings.Add("AG4MCGMWB0", "AG4MCGMWB0");
            sqlBulkCopy.ColumnMappings.Add("GMABEDATE", "GMABEDATE");
            sqlBulkCopy.ColumnMappings.Add("GMABSIG", "GMABSIG");
            sqlBulkCopy.ColumnMappings.Add("AG4MCGMAB0", "AG4MCGMAB0");
            sqlBulkCopy.ColumnMappings.Add("GMAWBAMNT", "GMAWBAMNT");
            sqlBulkCopy.ColumnMappings.Add("GMAWBMA", "GMAWBMA");
            sqlBulkCopy.ColumnMappings.Add("GMAWBRA", "GMAWBRA");
            sqlBulkCopy.ColumnMappings.Add("ECPWAIVE", "ECPWAIVE");
            sqlBulkCopy.ColumnMappings.Add("ISTATE", "ISTATE");
            sqlBulkCopy.ColumnMappings.Add("GROUPID", "GROUPID");
            sqlBulkCopy.ColumnMappings.Add("ACCTTYPE", "ACCTTYPE");
            sqlBulkCopy.ColumnMappings.Add("GALSEG", "GALSEG");
            sqlBulkCopy.ColumnMappings.Add("ADMNSTAT", "ADMNSTAT");
            sqlBulkCopy.ColumnMappings.Add("INACTRANS", "INACTRANS");
            sqlBulkCopy.ColumnMappings.Add("SCWAIVE", "SCWAIVE");
            sqlBulkCopy.ColumnMappings.Add("AGENT", "AGENT");
            sqlBulkCopy.ColumnMappings.Add("VARIANT", "VARIANT");
            sqlBulkCopy.ColumnMappings.Add("GCOMMID", "GCOMMID");
            sqlBulkCopy.ColumnMappings.Add("SLCRE", "SLCRE");
            sqlBulkCopy.ColumnMappings.Add("EITERMDATE", "EITERMDATE");
            sqlBulkCopy.ColumnMappings.Add("EITERM", "EITERM");
            sqlBulkCopy.ColumnMappings.Add("EIPRATE", "EIPRATE");
            sqlBulkCopy.ColumnMappings.Add("EICAP", "EICAP");
            sqlBulkCopy.ColumnMappings.Add("EIOV0", "EIOV0");
            sqlBulkCopy.ColumnMappings.Add("EIPRATEM", "EIPRATEM");
            sqlBulkCopy.ColumnMappings.Add("EI_INDEX0", "EI_INDEX0");
            sqlBulkCopy.ColumnMappings.Add("EI_INDEXHW", "EI_INDEXHW");
            sqlBulkCopy.ColumnMappings.Add("EIOV_BOT", "EIOV_BOT");
            sqlBulkCopy.ColumnMappings.Add("EIOV_PA", "EIOV_PA");
            sqlBulkCopy.ColumnMappings.Add("EIOV_VD", "EIOV_VD");
            sqlBulkCopy.ColumnMappings.Add("EIFVMINBOT", "EIFVMINBOT");
            sqlBulkCopy.ColumnMappings.Add("EIFVBOT", "EIFVBOT");
            sqlBulkCopy.ColumnMappings.Add("EIPRATE2", "EIPRATE2");
            sqlBulkCopy.ColumnMappings.Add("EIPRLMT", "EIPRLMT");
            sqlBulkCopy.ColumnMappings.Add("KP_REMPREM", "KP_REMPREM");
            sqlBulkCopy.ColumnMappings.Add("KP_SPCRED", "KP_SPCRED");
            sqlBulkCopy.ColumnMappings.Add("ICMIN2", "ICMIN2");
            sqlBulkCopy.ColumnMappings.Add("FVMIN2LD", "FVMIN2LD");
            sqlBulkCopy.ColumnMappings.Add("EI_VAL_LST_ANNI", "EI_VAL_LST_ANNI");
            sqlBulkCopy.ColumnMappings.Add("CARVM_OVRT", "CARVM_OVRT");
            sqlBulkCopy.ColumnMappings.Add("DLCOHORT", "DLCOHORT");
            sqlBulkCopy.ColumnMappings.Add("RECORDIDA", "RECORDIDA");

        }
        protected void FillDataTable(DataTable outputData, string conString)
        {

            using (TextReader reader = new StreamReader(conString))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    string[] headers = csv.Read();
                    while (true)
                    {
                        string[] row = csv.Read();
                        if (row == null)
                        {
                            break;
                        }
                        DataRow toInsert = outputData.NewRow();
                        int i = 0;
                        foreach (DataColumn col in outputData.Columns)
                        {
                            if (row[i] != "" && outputData.Columns.Contains(headers[i]))
                            {
                                if (col.DataType == typeof(DateTime) && row[i] == "00/00/0000")
                                {
                                    i++;
                                }
                                else
                                {
                                    toInsert[headers[i]] = Convert.ChangeType(row[i++], col.DataType);
                                }
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
