using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Extensions
{
    public static class Extensions
    {
        public static void ToCSV(this DataTable dtDataTable, string strFilePath, bool onlyDatePart = false)
        {
            StreamWriter sw = new StreamWriter(strFilePath, false);
            //headers  
            for (int i = 0; i < dtDataTable.Columns.Count; i++)
            {
                sw.Write(dtDataTable.Columns[i]);
                if (i < dtDataTable.Columns.Count - 1)
                {
                    sw.Write(",");
                }
            }
            sw.Write(sw.NewLine);
            foreach (DataRow dr in dtDataTable.Rows)
            {
                for (int i = 0; i < dtDataTable.Columns.Count; i++)
                {
                    if (!Convert.IsDBNull(dr[i]))
                    {
                        string valueToWrite;
                        if (onlyDatePart && dr[i] is DateTime)
                        {
                            DateTime date = (DateTime)dr[i];
                            valueToWrite = date.ToString("M/d/yyyy");
                        }
                        else
                        {
                            valueToWrite = dr[i].ToString();
                        }
                        if (valueToWrite.Contains(','))
                        {
                            valueToWrite = String.Format("\"{0}\"", valueToWrite);
                            sw.Write(valueToWrite);
                        }
                        else
                        {
                            sw.Write(valueToWrite);
                        }
                    }
                    if (i < dtDataTable.Columns.Count - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.Close();
        }
        public static bool CaseInsensitiveContains(this string text, string value,
            StringComparison stringComparison = StringComparison.CurrentCultureIgnoreCase)
        {
            return text.IndexOf(value, stringComparison) >= 0;
        }
        public static string CheckFolderPath(this string folderPath)
        {
            if (string.IsNullOrWhiteSpace(folderPath)) return folderPath;
            string rightMostChar = folderPath.Substring(folderPath.Length - 1);
            if (rightMostChar != "\\")
            {
                folderPath += "\\";
            }
            System.IO.Directory.CreateDirectory(folderPath);
            return folderPath;
        }
        public static bool IsNumericType(this Type t)
        {
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
        public static DateTime? GetFirstDateFromString(string inputText, string regexStr, string dateTimeFormat)
        {

            var regex = new Regex(regexStr);
            foreach (Match m in regex.Matches(inputText))
            {
                if (DateTime.TryParseExact(m.Value, dateTimeFormat, CultureInfo.CurrentCulture, DateTimeStyles.None, out DateTime dt))
                    return dt;
            }
            return null;
        }
    }
}
