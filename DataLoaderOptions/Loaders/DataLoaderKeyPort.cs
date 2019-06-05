using CsvHelper;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DataLoaderOptions
{
    class DataLoaderKeyPort : DataLoader
    {
        string _folderPath;
        string _zipstart;
        bool _hasHeaders;
        static object toLock = new object();
        public DataLoaderKeyPort()
        {
            _folderPath = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Keyport\ToUpload\";
            OutputPath = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\DLIC Keyport\";
            _hasHeaders = true;
        }
        public override string SqlTableName => "DLIC.PoliciesKeyport";
        public override void LoadToSql()
        {
            string[] files = Directory.GetFiles(_folderPath, "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files)
            {
                string outputPath = OutputPath + Path.GetFileName(file);
                if (Path.GetExtension(file).Equals(".txt", StringComparison.InvariantCultureIgnoreCase))
                {
                    Upload(file, SqlTableName, _hasHeaders);
                    if (File.Exists(outputPath))
                    {
                        File.Delete(outputPath);
                    }
                    File.Move(file, outputPath);
                }
            }
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
                        try
                        {
                            sqlBulkCopy.BulkCopyTimeout = 0;
                            sqlBulkCopy.WriteToServer(outputData);
                        }
                        catch (SqlException ex)
                        {
                            if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
                            {
                                string pattern = @"\d+";
                                Match match = Regex.Match(ex.Message.ToString(), pattern);
                                var index = Convert.ToInt32(match.Value) - 1;

                                FieldInfo fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings", BindingFlags.NonPublic | BindingFlags.Instance);
                                var sortedColumns = fi.GetValue(sqlBulkCopy);
                                var items = (Object[])sortedColumns.GetType().GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(sortedColumns);

                                FieldInfo itemdata = items[index].GetType().GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                                var metadata = itemdata.GetValue(items[index]);

                                var column = metadata.GetType().GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                                var length = metadata.GetType().GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).GetValue(metadata);
                                throw new FormatException(String.Format("Column: {0} contains data with a length greater than: {1}", column, length));
                            }

                            throw;
                        }
                    }
                }
                if (ToLoad)
                {
                    try
                    {
                        using (SqlCommand cmd = new SqlCommand("DLIC.InsertKeyport", con))
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
                    con.Close();
                }
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
            tbl.Columns.Add("GROUPNO", typeof(int));
            tbl.Columns.Add("SEGMENT", typeof(int));
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
            tbl.Columns.Add("X", typeof(int));
            tbl.Columns.Add("Y", typeof(int));
            tbl.Columns.Add("XA", typeof(int));
            tbl.Columns.Add("YA", typeof(int));
            tbl.Columns.Add("SEXX", typeof(string));
            tbl.Columns.Add("SEXY", typeof(string));
            tbl.Columns.Add("SEXXA", typeof(string));
            tbl.Columns.Add("SEXYA", typeof(string));
            tbl.Columns.Add("GCANN0", typeof(double));
            tbl.Columns.Add("MODE", typeof(string));
            tbl.Columns.Add("FLEXIBLE", typeof(string));
            tbl.Columns.Add("MPY", typeof(int));
            tbl.Columns.Add("FV1_PRIOR", typeof(double));
            tbl.Columns.Add("FV2_PRIOR", typeof(double));
            tbl.Columns.Add("FV2", typeof(double));
            tbl.Columns.Add("CV0", typeof(double));
            tbl.Columns.Add("LV0", typeof(double));
            tbl.Columns.Add("GIDATE", typeof(DateTime));
            tbl.Columns.Add("GIRATE0", typeof(double));
            tbl.Columns.Add("GIRATE1", typeof(double));
            tbl.Columns.Add("IBAILOUT0", typeof(double));
            tbl.Columns.Add("SUMGC0", typeof(double));
            tbl.Columns.Add("SUMPW0", typeof(double));
            tbl.Columns.Add("YTDGCF", typeof(double));
            tbl.Columns.Add("YTDGCR", typeof(double));
            tbl.Columns.Add("YTDPWF", typeof(double));
            tbl.Columns.Add("YTDXFV", typeof(double));
            tbl.Columns.Add("BDOPT", typeof(int));
            tbl.Columns.Add("RABD0", typeof(double));
            tbl.Columns.Add("RUBD0", typeof(double));
            tbl.Columns.Add("EABD0", typeof(double));
            tbl.Columns.Add("MVA0", typeof(double));
            tbl.Columns.Add("MVAI", typeof(double));
            tbl.Columns.Add("MVAFRATE", typeof(double));
            tbl.Columns.Add("XIMECHARGE", typeof(double));
            tbl.Columns.Add("EITERMDATE", typeof(DateTime));
            tbl.Columns.Add("EITERM", typeof(int));
            tbl.Columns.Add("EIPRATE", typeof(double));
            tbl.Columns.Add("EICAP", typeof(double));
            tbl.Columns.Add("EIOV0", typeof(double));
            tbl.Columns.Add("EIPRATEM", typeof(double));
            tbl.Columns.Add("GMIBEDATE", typeof(DateTime));
            tbl.Columns.Add("GMIBDATE", typeof(DateTime));
            tbl.Columns.Add("BDFACE", typeof(double));
            tbl.Columns.Add("BDFACP", typeof(double));
            tbl.Columns.Add("BIRATE0", typeof(double));
            tbl.Columns.Add("BIRATEADJ", typeof(double));
            tbl.Columns.Add("BISDATE", typeof(DateTime));
            tbl.Columns.Add("BIEDATE", typeof(DateTime));
            tbl.Columns.Add("BTIRATE", typeof(double));
            tbl.Columns.Add("GIRATEU", typeof(double));
            tbl.Columns.Add("GIDATEU", typeof(string));
            tbl.Columns.Add("GIRATEU1", typeof(double));
            tbl.Columns.Add("GCOMMID", typeof(string));
            tbl.Columns.Add("RS_NH", typeof(string));
            tbl.Columns.Add("REBD0", typeof(double));
            tbl.Columns.Add("RABA0", typeof(double));
            tbl.Columns.Add("RUBA0", typeof(double));
            tbl.Columns.Add("EI_INDEX0", typeof(double));
            tbl.Columns.Add("EI_INDEXHW", typeof(double));
            tbl.Columns.Add("RCBD0", typeof(double));
            tbl.Columns.Add("RCBD1", typeof(double));
            tbl.Columns.Add("RCBD10", typeof(double));
            tbl.Columns.Add("RCBA0", typeof(double));
            tbl.Columns.Add("EIOV_BOT", typeof(double));
            tbl.Columns.Add("EIOV_PA", typeof(string));
            tbl.Columns.Add("EI_INDEXAV", typeof(double));
            tbl.Columns.Add("EIOV_VD", typeof(string));
            tbl.Columns.Add("BD2OPT", typeof(int));
            tbl.Columns.Add("BD2AGE", typeof(double));
            tbl.Columns.Add("EIFVMINBOT", typeof(double));
            tbl.Columns.Add("EIFVBOT", typeof(double));
            tbl.Columns.Add("QUOTA_SH", typeof(double));
            tbl.Columns.Add("GMGDBIND", typeof(int));
            tbl.Columns.Add("SLCODDATE", typeof(DateTime));
            tbl.Columns.Add("ISTATE", typeof(string));
            tbl.Columns.Add("KP_REMPREM", typeof(double));
            tbl.Columns.Add("KP_SPCRED", typeof(double));
            tbl.Columns.Add("SLC_FUNDID", typeof(int));
            tbl.Columns.Add("SFNAME", typeof(string));
            tbl.Columns.Add("POL_SEQ", typeof(int));
            tbl.Columns.Add("KP_FNDPRM0", typeof(double));
            tbl.Columns.Add("ICMIN2", typeof(double));
            tbl.Columns.Add("FVMIN2LD", typeof(double));
            tbl.Columns.Add("MLT_UT_DGV", typeof(double));
            tbl.Columns.Add("MLT_UT_CV0", typeof(double));
            tbl.Columns.Add("CARVM_OVRT", typeof(int));
            tbl.Columns.Add("DLCOHORT", typeof(string));
            tbl.Columns.Add("RECORDID", typeof(string));
            tbl.Columns.Add("FAST_POL", typeof(string));
            tbl.Columns.Add("FAST_STATUS", typeof(string));
            tbl.Columns.Add("SNFL_VALUE", typeof(string));

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
            sqlBulkCopy.ColumnMappings.Add("UserID", "UserID");
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
            sqlBulkCopy.ColumnMappings.Add("FV1_PRIOR", "FV1_PRIOR");
            sqlBulkCopy.ColumnMappings.Add("FV2_PRIOR", "FV2_PRIOR");
            sqlBulkCopy.ColumnMappings.Add("FV2", "FV2");
            sqlBulkCopy.ColumnMappings.Add("CV0", "CV0");
            sqlBulkCopy.ColumnMappings.Add("LV0", "LV0");
            sqlBulkCopy.ColumnMappings.Add("GIDATE", "GIDATE");
            sqlBulkCopy.ColumnMappings.Add("GIRATE0", "GIRATE0");
            sqlBulkCopy.ColumnMappings.Add("GIRATE1", "GIRATE1");
            sqlBulkCopy.ColumnMappings.Add("IBAILOUT0", "IBAILOUT0");
            sqlBulkCopy.ColumnMappings.Add("SUMGC0", "SUMGC0");
            sqlBulkCopy.ColumnMappings.Add("SUMPW0", "SUMPW0");
            sqlBulkCopy.ColumnMappings.Add("YTDGCF", "YTDGCF");
            sqlBulkCopy.ColumnMappings.Add("YTDGCR", "YTDGCR");
            sqlBulkCopy.ColumnMappings.Add("YTDPWF", "YTDPWF");
            sqlBulkCopy.ColumnMappings.Add("YTDXFV", "YTDXFV");
            sqlBulkCopy.ColumnMappings.Add("BDOPT", "BDOPT");
            sqlBulkCopy.ColumnMappings.Add("RABD0", "RABD0");
            sqlBulkCopy.ColumnMappings.Add("RUBD0", "RUBD0");
            sqlBulkCopy.ColumnMappings.Add("EABD0", "EABD0");
            sqlBulkCopy.ColumnMappings.Add("MVA0", "MVA0");
            sqlBulkCopy.ColumnMappings.Add("MVAI", "MVAI");
            sqlBulkCopy.ColumnMappings.Add("MVAFRATE", "MVAFRATE");
            sqlBulkCopy.ColumnMappings.Add("XIMECHARGE", "XIMECHARGE");
            sqlBulkCopy.ColumnMappings.Add("EITERMDATE", "EITERMDATE");
            sqlBulkCopy.ColumnMappings.Add("EITERM", "EITERM");
            sqlBulkCopy.ColumnMappings.Add("EIPRATE", "EIPRATE");
            sqlBulkCopy.ColumnMappings.Add("EICAP", "EICAP");
            sqlBulkCopy.ColumnMappings.Add("EIOV0", "EIOV0");
            sqlBulkCopy.ColumnMappings.Add("EIPRATEM", "EIPRATEM");
            sqlBulkCopy.ColumnMappings.Add("GMIBEDATE", "GMIBEDATE");
            sqlBulkCopy.ColumnMappings.Add("GMIBDATE", "GMIBDATE");
            sqlBulkCopy.ColumnMappings.Add("BDFACE", "BDFACE");
            sqlBulkCopy.ColumnMappings.Add("BDFACP", "BDFACP");
            sqlBulkCopy.ColumnMappings.Add("BIRATE0", "BIRATE0");
            sqlBulkCopy.ColumnMappings.Add("BIRATEADJ", "BIRATEADJ");
            sqlBulkCopy.ColumnMappings.Add("BISDATE", "BISDATE");
            sqlBulkCopy.ColumnMappings.Add("BIEDATE", "BIEDATE");
            sqlBulkCopy.ColumnMappings.Add("BTIRATE", "BTIRATE");
            sqlBulkCopy.ColumnMappings.Add("GIRATEU", "GIRATEU");
            sqlBulkCopy.ColumnMappings.Add("GIDATEU", "GIDATEU");
            sqlBulkCopy.ColumnMappings.Add("GIRATEU1", "GIRATEU1");
            sqlBulkCopy.ColumnMappings.Add("GCOMMID", "GCOMMID");
            sqlBulkCopy.ColumnMappings.Add("RS_NH", "RS_NH");
            sqlBulkCopy.ColumnMappings.Add("REBD0", "REBD0");
            sqlBulkCopy.ColumnMappings.Add("RABA0", "RABA0");
            sqlBulkCopy.ColumnMappings.Add("RUBA0", "RUBA0");
            sqlBulkCopy.ColumnMappings.Add("EI_INDEX0", "EI_INDEX0");
            sqlBulkCopy.ColumnMappings.Add("EI_INDEXHW", "EI_INDEXHW");
            sqlBulkCopy.ColumnMappings.Add("RCBD0", "RCBD0");
            sqlBulkCopy.ColumnMappings.Add("RCBD1", "RCBD1");
            sqlBulkCopy.ColumnMappings.Add("RCBD10", "RCBD10");
            sqlBulkCopy.ColumnMappings.Add("RCBA0", "RCBA0");
            sqlBulkCopy.ColumnMappings.Add("EIOV_BOT", "EIOV_BOT");
            sqlBulkCopy.ColumnMappings.Add("EIOV_PA", "EIOV_PA");
            sqlBulkCopy.ColumnMappings.Add("EI_INDEXAV", "EI_INDEXAV");
            sqlBulkCopy.ColumnMappings.Add("EIOV_VD", "EIOV_VD");
            sqlBulkCopy.ColumnMappings.Add("BD2OPT", "BD2OPT");
            sqlBulkCopy.ColumnMappings.Add("BD2AGE", "BD2AGE");
            sqlBulkCopy.ColumnMappings.Add("EIFVMINBOT", "EIFVMINBOT");
            sqlBulkCopy.ColumnMappings.Add("EIFVBOT", "EIFVBOT");
            sqlBulkCopy.ColumnMappings.Add("QUOTA_SH", "QUOTA_SH");
            sqlBulkCopy.ColumnMappings.Add("GMGDBIND", "GMGDBIND");
            sqlBulkCopy.ColumnMappings.Add("SLCODDATE", "SLCODDATE");
            sqlBulkCopy.ColumnMappings.Add("ISTATE", "ISTATE");
            sqlBulkCopy.ColumnMappings.Add("KP_REMPREM", "KP_REMPREM");
            sqlBulkCopy.ColumnMappings.Add("KP_SPCRED", "KP_SPCRED");
            sqlBulkCopy.ColumnMappings.Add("SLC_FUNDID", "SLC_FUNDID");
            sqlBulkCopy.ColumnMappings.Add("SFNAME", "SFNAME");
            sqlBulkCopy.ColumnMappings.Add("POL_SEQ", "POL_SEQ");
            sqlBulkCopy.ColumnMappings.Add("KP_FNDPRM0", "KP_FNDPRM0");
            sqlBulkCopy.ColumnMappings.Add("ICMIN2", "ICMIN2");
            sqlBulkCopy.ColumnMappings.Add("FVMIN2LD", "FVMIN2LD");
            sqlBulkCopy.ColumnMappings.Add("MLT_UT_DGV", "MLT_UT_DGV");
            sqlBulkCopy.ColumnMappings.Add("MLT_UT_CV0", "MLT_UT_CV0");
            sqlBulkCopy.ColumnMappings.Add("CARVM_OVRT", "CARVM_OVRT");
            sqlBulkCopy.ColumnMappings.Add("DLCOHORT", "DLCOHORT");
            sqlBulkCopy.ColumnMappings.Add("RECORDID", "RECORDID");
            sqlBulkCopy.ColumnMappings.Add("FAST_POL", "FAST_POL");
            sqlBulkCopy.ColumnMappings.Add("FAST_STATUS", "FAST_STATUS");
            sqlBulkCopy.ColumnMappings.Add("SNFL_VALUE", "SNFL_VALUE");
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
                            if (row[i] != "" && col.ColumnName.Equals(headers[i], StringComparison.InvariantCultureIgnoreCase))
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
