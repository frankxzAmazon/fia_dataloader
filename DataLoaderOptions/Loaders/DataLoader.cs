using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DataLoaderOptions
{
    abstract class DataLoader : IDataLoader
    {
        /// <summary>
        /// Gets or sets a value indicating whether we want [to load] the data.
        /// </summary>
        /// <value>
        ///   <c>true</c> if we want [to load]; otherwise, <c>false</c>.
        /// </value>
        public bool ToLoad { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether we're in testing mode or not. If we're in testing mode,
        /// then we have to load the data to the development database.
        /// </summary>
        /// <value>
        ///   <c>true</c> if [testing mode]; otherwise, <c>false</c>.
        /// </value>
        public bool useDevDB { get; set; }
        public abstract string SqlTableName { get; }
        public string OutputPath { get; protected set; }
        public bool CheckSource()
        {
            //checks if all 4 sources are in the database before loading DLICNBFIA
            string connectionString = ConfigurationManager.ConnectionStrings["Sql"].ConnectionString;
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                con.Open();
                SqlCommand sourceCount = new SqlCommand("select count(distinct source) as NumSource from DLIC.PoliciesNBFIAIssueState", con);
                int numCount = System.Convert.ToInt32(sourceCount.ExecuteScalar());
                return numCount == 4;
            }
        }

        public virtual void LoadData(DataTable outputData)
        {
            string connection = useDevDB ? "Testing" : "Staging";
            string sqlString = ConfigurationManager.ConnectionStrings[connection].ConnectionString;
            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();
                using (SqlCommand cmd = new SqlCommand($"delete from {SqlTableName}", con))
                {
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("delete from dbo.OptionInventoryStagingTable", con))
                {
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                }

                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                {
                    //Set the database table name
                    sqlBulkCopy.BulkCopyTimeout = 0;
                    sqlBulkCopy.DestinationTableName = SqlTableName;
                    MapTable(sqlBulkCopy);
                    try
                    {
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
                con.Close();
            }
        }

        public abstract void LoadToSql();

        protected int CreateChangeLog(string source, string objectName, string objectType, DateTime effectiveDate, SqlTransaction transaction = null)
        {

            string sqlString = ConfigurationManager.ConnectionStrings["SQL"].ConnectionString;
            int id;

            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();
                SqlTransaction trans = transaction ?? con.BeginTransaction();
                using (SqlCommand cmd = new SqlCommand("Select MAX(idChangeLog) From dbo.ChangeLog", con, trans))
                {
                    id = (Int32)cmd.ExecuteScalar() + 1;
                }
                string cmdString = "INSERT INTO dbo.ChangeLog (idChangeLog,LoadDate,UserId, Source, ObjectName, ObjectType,Effectivedate) " +
                    "VALUES (@val1, @va2, @val3, @val4, @val5, @val6, @val7)";
                using (SqlCommand cmd = new SqlCommand(cmdString, con, trans))
                {
                    cmd.CommandText = cmdString;
                    cmd.Parameters.AddWithValue("@val1", id);
                    cmd.Parameters.AddWithValue("@val2", DateTime.UtcNow);
                    cmd.Parameters.AddWithValue("@val3", Environment.UserName);
                    cmd.Parameters.AddWithValue("@val4", source);
                    cmd.Parameters.AddWithValue("@val5", objectName);
                    cmd.Parameters.AddWithValue("@val6", objectType);
                    cmd.Parameters.AddWithValue("@val7", effectiveDate);
                }
                if (transaction == null) trans.Commit();
                con.Close();
            }
            return id;
        }
        protected string GetConnString(string filePath, bool hasHeader)
        {
            string header;
            if (hasHeader)
            {
                header = "Yes";
            }
            else
            {
                header = "No";
            }
            string extension = Path.GetExtension(filePath);
            string conString = string.Empty;
            switch (extension)
            {
                case ".xls":
                    conString = ConfigurationManager.ConnectionStrings["Excel03ConString"].ConnectionString;
                    break;
                case ".xlsx":
                    conString = ConfigurationManager.ConnectionStrings["Excel07+ConString"].ConnectionString;
                    break;
                default:
                    return filePath;
            }
            return string.Format(conString, filePath, header);
        }
        protected abstract void MapTable(SqlBulkCopy sqlBulkCopy);
        //protected virtual void FillDataTable(DataTable outputData, string conString)
        //{
        //    using (OleDbConnection con = new OleDbConnection(conString))
        //    {
        //        con.Open();
        //        string sheet1 = con.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null).Rows[0]["TABLE_NAME"].ToString();

        //        using (OleDbDataAdapter oda = new OleDbDataAdapter("SELECT * FROM [" + sheet1 + "]", con))
        //        {
        //            oda.Fill(outputData);
        //        }
        //    }
        //}
    }
}
