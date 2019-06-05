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

namespace DataLoaderOptions
{
    abstract class DataReaderExcel : IDataReader
    {
        protected string _currentFile;
        public string OutputPath { get; protected set; }
        public virtual void ReadData(string filePath, DateTime asOfDate, string sqlTable, bool hasHeader)
        {
            string sqlString = ConfigurationManager.ConnectionStrings["SQL"].ConnectionString;
            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();
                using (SqlCommand sqlCommand = new SqlCommand("Delete from " + sqlTable + " where InforceDate like '" + asOfDate.ToShortDateString() + "'",con))
                {
                    sqlCommand.ExecuteNonQuery();
                }
                DataTable outputData = CreateDataTable(asOfDate);
                FillDataTable(outputData, GetConnString(filePath, hasHeader));
                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                {
                    //Set the database table name
                    sqlBulkCopy.DestinationTableName = sqlTable;
                    MapTable(sqlBulkCopy);
                    CorrectDataTable(outputData);
                    sqlBulkCopy.WriteToServer(outputData);
                }
                con.Close();
            }
            File.Move(filePath, OutputPath + Path.GetFileName(filePath));
        }
        public virtual void ReadData(string filePath, string sqlTable, bool hasHeader)
        {
            string sqlString = ConfigurationManager.ConnectionStrings["SQL"].ConnectionString;
            DataTable outputData = CreateDataTable();
            FillDataTable(outputData, GetConnString(filePath, hasHeader));
            DateTime asOfDate = outputData.Rows[2].Field<DateTime>("InforceDate");
            using (SqlConnection con = new SqlConnection(sqlString))
            {
                con.Open();
                Int32 count;
                //using (SqlCommand sqlCommand = new SqlCommand("Delete from " + sqlTable + " where InforceDate like '" + asOfDate.ToShortDateString() + "'", con))
                //{
                //    sqlCommand.ExecuteNonQuery();
                //}
                using (SqlCommand sqlCommand = new SqlCommand("select count(*) from " + sqlTable + " where InforceDate like '" + asOfDate.ToShortDateString() + "'", con))
                {
                    count = (Int32) sqlCommand.ExecuteScalar();
                }
                if (count == 0)
                {
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                    {
                        //Set the database table name
                        sqlBulkCopy.DestinationTableName = sqlTable;
                        MapTable(sqlBulkCopy);
                        CorrectDataTable(outputData);
                        sqlBulkCopy.WriteToServer(outputData);
                    }
                }
                con.Close();
            }
            File.Move(filePath, OutputPath + Path.GetFileName(filePath));
        }
        protected string GetConnString(string filePath, bool hasHeader)
        {
            string header;
            if(hasHeader)
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
        public abstract void ReadData();
        protected abstract DataTable CreateDataTable(DateTime defaultDate);
        protected abstract DataTable CreateDataTable();
        protected abstract void MapTable(SqlBulkCopy sqlBulkCopy);
        protected abstract void CorrectDataTable(DataTable outputData);
        protected virtual void FillDataTable(DataTable outputData, string conString)
        {
            using (OleDbConnection con = new OleDbConnection(conString))
            {
                con.Open();
                string sheet1 = con.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null).Rows[0]["TABLE_NAME"].ToString();

                using (OleDbDataAdapter oda = new OleDbDataAdapter("SELECT * FROM [" + sheet1 + "]", con))
                {
                    oda.Fill(outputData);
                }
            }
        }
    }
}
