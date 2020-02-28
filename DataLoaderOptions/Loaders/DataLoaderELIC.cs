using CsvHelper;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using log4net;
namespace DataLoaderOptions
{
    class DataLoaderELIC : DataLoader
    {
        private static readonly ILog log = LogManager.GetLogger(Environment.MachineName);
        string _folderPath;
        //string fileSubstring = ".csv";
        bool _hasHeaders;

        public override string SqlTableName => "GIS.PoliciesELIC";

        public DataLoaderELIC()
        {
            _folderPath = @"\\10.33.54.170\nas6\actuary\DACT\ALM\FIAHedging\DBUpload\ELIC FIA\ToUpload\";
            OutputPath = @"W:\DACT\ALM\FIAHedging\DBUpload\ELIC FIA\";
            _hasHeaders = false;
        }
        public void LoadToSql(string file)
        {
            string fileName = OutputPath + Path.GetFileName(file);
            DataTable outputData = CreateDataTable();
            FillDataTable(outputData, GetConnString(file, true), fileName);
            outputData.AcceptChanges();
            DateTime asOfDate = outputData.AsEnumerable().Select(x => x.Field<DateTime>("InforceDate")).Max();
            string outfile = OutputPath + "EqHedge_" + asOfDate.ToString("yyyyMMdd") + ".csv";
            UpdateAsOfDate(outputData, outfile);


            string sqlString = ConfigurationManager.ConnectionStrings["Staging"].ConnectionString;
            using (SqlConnection con = new SqlConnection(sqlString))
            {
                try
                {
                    con.Open();
                    using (SqlCommand cmd = new SqlCommand($"delete from {SqlTableName}", con))
                    {
                        //   cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 0;
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlCommand cmd = new SqlCommand("delete from OptionInventoryStagingTable", con))
                    {
                        //   cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = 0;
                        cmd.ExecuteNonQuery();
                    }

                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {
                        //Set the database table name
                        sqlBulkCopy.BulkCopyTimeout = 0;
                        sqlBulkCopy.DestinationTableName = SqlTableName;
                        MapTable(sqlBulkCopy);
                        sqlBulkCopy.WriteToServer(outputData);
                    }
                    if (ToLoad)
                    {
                        using (SqlCommand cmd = new SqlCommand("GIS.InsertELIC", con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            cmd.CommandTimeout = 0;
                            cmd.ExecuteNonQuery();
                        }

                    }
                }
                catch (Exception ex)

                {
                    log.Fatal("Error with DataLoader ELIC" + asOfDate);
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    if (File.Exists(outfile))
                    {
                        File.Delete(outfile);
                    }
                    File.Move(file, outfile);
                    con.Close();
                }
            }
        }
        public override void LoadToSql()
        {
            string[] files = Directory.GetFiles(_folderPath, "*", SearchOption.TopDirectoryOnly);
            foreach (string file in files.OrderBy(x => x).ToList())
            {
                LoadToSql(file);
            }
        }
        protected DataTable CreateDataTable()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("LoadDate", typeof(DateTime));
            tbl.Columns.Add("Source", typeof(string));
            tbl.Columns.Add("UserId", typeof(string));
            tbl.Columns.Add("Contract", typeof(string));
            tbl.Columns.Add("PolicyDate", typeof(DateTime));
            tbl.Columns.Add("PolicyAnniversary", typeof(DateTime));
            tbl.Columns.Add("IncomeDate", typeof(DateTime));
            tbl.Columns.Add("PolicyDuration", typeof(int));
            tbl.Columns.Add("OwnerAge", typeof(int));
            tbl.Columns.Add("OwnerSex", typeof(string));
            tbl.Columns.Add("AnnuitantAge", typeof(int));
            tbl.Columns.Add("AnnuitantSex", typeof(string));
            tbl.Columns.Add("JointSex", typeof(string));
            tbl.Columns.Add("JointAge", typeof(int));
            tbl.Columns.Add("MinimumRate", typeof(decimal));
            tbl.Columns.Add("MVABaseRate", typeof(decimal));
            tbl.Columns.Add("IndexValue", typeof(decimal));
            tbl.Columns.Add("PriorAnniversaryValue", typeof(decimal));
            tbl.Columns.Add("WdsSinceLastAnniv", typeof(decimal));
            tbl.Columns.Add("AccountValue", typeof(decimal));
            tbl.Columns.Add("MinimumAccountValue", typeof(decimal));
            tbl.Columns.Add("IndexCap", typeof(decimal));
            tbl.Columns.Add("IndexTerm", typeof(int));
            tbl.Columns.Add("IndexMinimumCap", typeof(decimal));
            tbl.Columns.Add("ParticipationRate", typeof(decimal));
            tbl.Columns.Add("IndexMargin", typeof(decimal));
            tbl.Columns.Add("Fund", typeof(string));
            tbl.Columns.Add("InforceDate", typeof(DateTime));
            tbl.Columns.Add("AllocationPercentage", typeof(decimal));
            tbl.Columns.Add("FixedInterestRate", typeof(decimal));
            tbl.Columns.Add("RenewalDate", typeof(DateTime));
            tbl.Columns.Add("TreatyGroupNumber", typeof(string));
            tbl.Columns["TreatyGroupNumber"].DefaultValue = "";
            tbl.Columns.Add("Coinsurance Percentage", typeof(decimal));
            tbl.Columns.Add("Ceded Value", typeof(decimal));
            tbl.Columns.Add("TreatyGroupNumber2", typeof(string));
            tbl.Columns["TreatyGroupNumber2"].DefaultValue = "";
            tbl.Columns.Add("Coinsurance Percentage2", typeof(decimal));
            tbl.Columns.Add("Ceded Value2", typeof(decimal));
            tbl.Columns.Add("HedgingPercentage", typeof(decimal));
            tbl.Columns.Add("Budget", typeof(decimal));
            tbl.Columns["Budget"].DefaultValue = 0;

            return tbl;
        }
        protected override void MapTable(SqlBulkCopy sqlBulkCopy)
        {
            //[OPTIONAL]: Map the Excel columns with that of the database table
            sqlBulkCopy.ColumnMappings.Add("LoadDate", "LoadDate");
            sqlBulkCopy.ColumnMappings.Add("Source", "Source");
            sqlBulkCopy.ColumnMappings.Add("UserId", "UserID");
            sqlBulkCopy.ColumnMappings.Add("Contract", "Contract");
            sqlBulkCopy.ColumnMappings.Add("PolicyDate", "PolicyDate");
            sqlBulkCopy.ColumnMappings.Add("PolicyAnniversary", "PolicyAnniversary");
            sqlBulkCopy.ColumnMappings.Add("IncomeDate", "IncomeDate");
            sqlBulkCopy.ColumnMappings.Add("PolicyDuration", "PolicyDuration");
            sqlBulkCopy.ColumnMappings.Add("OwnerAge", "OwnerAge");
            sqlBulkCopy.ColumnMappings.Add("OwnerSex", "OwnerSex");
            sqlBulkCopy.ColumnMappings.Add("AnnuitantAge", "AnnuitantAge");
            sqlBulkCopy.ColumnMappings.Add("AnnuitantSex", "AnnuitantSex");
            sqlBulkCopy.ColumnMappings.Add("JointSex", "JointSex");
            sqlBulkCopy.ColumnMappings.Add("JointAge", "JointAge");
            sqlBulkCopy.ColumnMappings.Add("MinimumRate", "MinimumRate");
            sqlBulkCopy.ColumnMappings.Add("MVABaseRate", "MVABaseRate");
            sqlBulkCopy.ColumnMappings.Add("IndexValue", "IndexValue");
            sqlBulkCopy.ColumnMappings.Add("PriorAnniversaryValue", "PriorAnniversaryValue");
            sqlBulkCopy.ColumnMappings.Add("WdsSinceLastAnniv", "WdsSinceLastAnniv");
            sqlBulkCopy.ColumnMappings.Add("AccountValue", "AccountValue");
            sqlBulkCopy.ColumnMappings.Add("MinimumAccountValue", "MinimumAccountValue");
            sqlBulkCopy.ColumnMappings.Add("IndexCap", "IndexCap");
            sqlBulkCopy.ColumnMappings.Add("IndexTerm", "IndexTerm");
            sqlBulkCopy.ColumnMappings.Add("IndexMinimumCap", "IndexMinimumCap");
            sqlBulkCopy.ColumnMappings.Add("ParticipationRate", "ParticipationRate");
            sqlBulkCopy.ColumnMappings.Add("IndexMargin", "IndexMargin");
            sqlBulkCopy.ColumnMappings.Add("Fund", "Fund");
            sqlBulkCopy.ColumnMappings.Add("InforceDate", "InforceDate");
            sqlBulkCopy.ColumnMappings.Add("AllocationPercentage", "AllocationPercentage");
            sqlBulkCopy.ColumnMappings.Add("FixedInterestRate", "FixedInterestRate");
            sqlBulkCopy.ColumnMappings.Add("RenewalDate", "RenewalDate");
            sqlBulkCopy.ColumnMappings.Add("TreatyGroupNumber", "TreatyGroupNumber");
            sqlBulkCopy.ColumnMappings.Add("Coinsurance Percentage", "Coinsurance Percentage");
            sqlBulkCopy.ColumnMappings.Add("Ceded Value", "Ceded Value");
            sqlBulkCopy.ColumnMappings.Add("TreatyGroupNumber2", "TreatyGroupNumber2");
            sqlBulkCopy.ColumnMappings.Add("Coinsurance Percentage2", "Coinsurance Percentage2");
            sqlBulkCopy.ColumnMappings.Add("Ceded Value2", "Ceded Value2");
            sqlBulkCopy.ColumnMappings.Add("HedgingPercentage", "HedgingPercentage");
            sqlBulkCopy.ColumnMappings.Add("Budget", "Budget");

        }

        protected void UpdateAsOfDate(DataTable outputData, string source)
        {
            DateTime loadDate = DateTime.UtcNow;
            foreach (DataRow row in outputData.AsEnumerable().ToList())
            {

                row["Source"] = source;
            }
            outputData.AcceptChanges();
        }

        protected void FillDataTable(DataTable outputData, string conString, string source = null)
        {
            DateTime now = DateTime.UtcNow;
            using (TextReader reader = new StreamReader(conString))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
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
                            else if (row[i] != "")
                            //if (row[i] != "")
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
