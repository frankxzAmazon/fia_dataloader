using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Excel = Microsoft.Office.Interop.Excel;
using Extensions;
using System.Globalization;
using System.Text.RegularExpressions;

namespace DataLoaderOptions.Readers
{
    public class DataReader_Excel
    {
        private int _headerRowBase0;
        private string _password = null;
        private Dictionary<string, string> _dateTimeFormats = new Dictionary<string, string>();
        public DataReader_Excel(string workbookPath, string worksheetName, int headerRowBase1, string password = null)
        {
            WorkbookPath = workbookPath;
            WorksheetName = worksheetName;
            _headerRowBase0 = headerRowBase1 - 1;
            _password = password;
        }
        public DataReader_Excel(string workbookPath, int headerRowBase1, int worksheetNumber = 1, string password = null)
        {
            WorkbookPath = workbookPath;
            WorkSheetIndex = worksheetNumber;
            _headerRowBase0 = headerRowBase1 - 1;
            _password = password;
        }
        public string WorkbookPath { get; }
        public string WorksheetName { get; private set; }
        public int WorkSheetIndex { get; private set; }
        public int HeaderRow { get { return _headerRowBase0 + 1; } set { _headerRowBase0 = value - 1; } }
        public int HeaderLength { get; set; } = 1;
        public string[] WorksheetColumns { get; set; }
        public DataTable DataTableFormat { get; private set; }
        public object[,] ExcelSheet { get; private set; }
        public void SetDataTableFormat()
        {
            if (ExcelSheet == null) LoadExcelFile();
            object[,] data = GetPartialArray(ExcelSheet, _headerRowBase0);
            DataTableFormat = CreateTableFromArray(data);
        }
        public void SetDateTimeFormat(string columnName, string datetimeFormat)
        {
            _dateTimeFormats[columnName] = datetimeFormat;
        }
        public void SetDataTableFormat(DataTable dt)
        {
            DataTableFormat = dt.Clone();
        }
        public void SetDataTableFormat(object[,] data)
        {
            DataTableFormat = CreateTableFromArray(data);
        }
        public void LoadExcelFile()
        {
            ExcelSheet = GetExcelSheetAsArray();
        }
        public void SetHeaderRow(int columnBase1, string searchValue)
        {
            if (ExcelSheet == null) LoadExcelFile();
            int headerRow = 1;
            int i = 1;
            while (headerRow == 1)
            {
                if ((ExcelSheet[i, columnBase1] as string)?.Equals(searchValue, StringComparison.InvariantCultureIgnoreCase) ?? false)
                {
                    headerRow = i;
                    _headerRowBase0 = i - 1;
                }
                else
                {
                    if (i++ == ExcelSheet.GetLength(0))
                    {
                        throw new KeyNotFoundException(searchValue + " not found");
                    }
                }
            }
        }
        public virtual DataTable GetFilledDataTable(OnError onError, bool hasHeader = true, DataTable template = null)
        {
            if (ExcelSheet == null) LoadExcelFile();
            object[,] data = GetPartialArray(ExcelSheet, _headerRowBase0);

            DataTable dt;
            if (hasHeader)
            {
                if (DataTableFormat == null) SetDataTableFormat(data);
                dt = DataTableFormat.Clone();
            }
            else
            {
                dt = template.Clone();
            }

            int rowCount = data.GetLength(0);
            int colCount = data.GetLength(1);
            if (hasHeader) WorksheetColumns = new string[colCount];

            var startingRow = hasHeader ? HeaderLength : 0;
            for (int r = startingRow; r < rowCount; r++)
            {
                DataRow row = dt.NewRow();
                bool toAdd = true;
                for (int c = 0; c < colCount; c++)
                {
                    int i = 1;
                    string columnName = data[0, c]?.ToString() ?? "";
                    while (i < HeaderLength)
                    {
                        columnName = string.Join(" ", columnName, data[i, c]?.ToString() ?? "");
                        i++;
                    }
                    columnName = Regex.Replace(columnName, @"\r\n?|\n", " ");
                    columnName = columnName.Trim(' ');
                    columnName = Regex.Replace(columnName, " +", " ");

                    if (hasHeader)
                    {
                        WorksheetColumns[c] = columnName;
                    }

                    if (hasHeader && dt.Columns.Contains(columnName))
                    {
                        try
                        {
                            if (data[r, c] == null && !dt.Columns[columnName].AllowDBNull)
                            {
                                toAdd = false;
                            }
                            else if (data[r, c] != null)
                            {
                                if (_dateTimeFormats.ContainsKey(columnName))
                                {
                                    string date = ((string)data[r, c]).Trim(' ');
                                    DateTime datedate = DateTime.ParseExact(date, _dateTimeFormats[columnName], CultureInfo.CurrentCulture);
                                    row[columnName] = datedate;
                                }
                                else
                                {
                                    row[columnName] = Convert.ChangeType(data[r, c], dt.Columns[columnName].DataType);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (onError == OnError.ThrowError) throw ex;
                        }
                    }

                    // Process the data a little differently if there are no headers. We have to
                    // assume that the order of the fields in the Excel file is the same as that
                    // of the DataTable.
                    if (!hasHeader)
                    {
                        columnName = WorksheetColumns[c];
                        var cellValue = data[r, c];

                        try
                        {
                            if (cellValue == null && !dt.Columns[columnName].AllowDBNull)
                            {
                                toAdd = false;
                            }
                            else if (cellValue != null)
                            {
                                if (_dateTimeFormats.ContainsKey(columnName))
                                {
                                    string date = ((string)cellValue).Trim(' ');
                                    DateTime datedate = DateTime.ParseExact(date, _dateTimeFormats[columnName], CultureInfo.CurrentCulture);
                                    row[columnName] = datedate;
                                }
                                else
                                {
                                    row[columnName] = Convert.ChangeType(cellValue, dt.Columns[columnName].DataType);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (onError == OnError.ThrowError) throw ex;
                        }
                    }

                }
                if (toAdd) { dt.Rows.Add(row); }
            }
            dt.AcceptChanges();
            return dt;
        }
        public object GetCell(int rowBase1, int columnBase1)
        {
            if (ExcelSheet == null) LoadExcelFile();
            return ExcelSheet[rowBase1, columnBase1];
        }
        private object[,] GetExcelSheetAsArray()
        {

            Excel.Application xlApp = new Excel.Application()
            {
                DisplayAlerts = false,
                Visible = false
            };
            Excel.Workbooks workbooks = xlApp.Workbooks;
            Excel.Workbook xlWorkbook;
            if (string.IsNullOrEmpty(_password))
            {
                xlWorkbook = workbooks.Open(WorkbookPath, UpdateLinks: false, ReadOnly: true, IgnoreReadOnlyRecommended: true);
            }
            else
            {
                xlWorkbook = workbooks.Open(WorkbookPath, UpdateLinks: false, ReadOnly: true, IgnoreReadOnlyRecommended: true, Password: _password);
            }

            Excel._Worksheet xlWorksheet = null;
            Excel.Range cells = null;
            Excel.Range last = null;
            Excel.Range rangeData = null;
            Excel.Range col = null;
            Excel.Range row = null;
            Console.WriteLine("Still working " + DateTime.Now.ToShortTimeString());
            try
            {

                if (string.IsNullOrWhiteSpace(WorksheetName)) xlWorksheet = xlWorkbook.Sheets[WorkSheetIndex];
                else xlWorksheet = xlWorkbook.Sheets[WorksheetName];
                WorksheetName = xlWorksheet.Name;
                WorkSheetIndex = xlWorksheet.Index;
                cells = xlWorksheet.Cells;
                try
                {
                    last = cells.SpecialCells(Excel.XlCellType.xlCellTypeLastCell, Type.Missing);
                    rangeData = xlWorksheet.get_Range("A1", last);
                }
                catch (System.Runtime.InteropServices.COMException)
                {
                    col = cells.Columns;
                    row = cells.Rows;
                    last = xlWorksheet.Cells[row.CountLarge, col.CountLarge];
                    rangeData = xlWorksheet.get_Range("A1", last);
                }
                object[,] data = rangeData.Value;
                return data;
            }
            finally
            {
                xlWorkbook.Close(SaveChanges: false);
                xlApp.Quit();
                //release com objects to fully kill excel process from running in the background
                if (xlApp != null) Marshal.FinalReleaseComObject(xlApp);
                if (workbooks != null) Marshal.FinalReleaseComObject(workbooks);
                if (xlWorkbook != null) Marshal.FinalReleaseComObject(xlWorkbook);
                if (xlWorksheet != null) Marshal.FinalReleaseComObject(xlWorksheet);
                if (cells != null) Marshal.FinalReleaseComObject(cells);
                if (last != null) Marshal.FinalReleaseComObject(last);
                if (rangeData != null) Marshal.FinalReleaseComObject(rangeData);
                if (col != null) Marshal.FinalReleaseComObject(col);
                if (row != null) Marshal.FinalReleaseComObject(row);
                xlApp = null;
                workbooks = null;
                xlWorkbook = null;
                xlWorksheet = null;
                cells = null;
                last = null;
                rangeData = null;
                //cleanup
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

        }
        private object[,] GetPartialArray(object[,] oldData, int headerRowBase0)
        {
            object[,] data = new object[oldData.GetLength(0) - headerRowBase0, oldData.GetLength(1)];
            int index1 = Get1DIndex(oldData, headerRowBase0) + 1;
            int index2 = Get1DIndex(data, data.GetLength(0));
            Array.Copy(oldData, index1, data, 0, index2);
            return data;
        }
        private int Get1DIndex(object[,] data, int row)
        {
            int colLength = data.GetLength(1);

            return row * colLength;
        }
        private DataTable CreateTableFromArray(object[,] data)
        {
            int rowCount = data.GetLength(0);
            int colCount = data.GetLength(1);
            DataTable dt = new DataTable();
            for (int c = 0; c < colCount; c++)
            {
                if (data[0, c] != null)
                {
                    Type t = data[1, c].GetType();
                    if (t == typeof(string)) break;
                    for (int r = 1; r < rowCount; r++)
                    {
                        if (t != data[r, c].GetType())
                        {
                            if (t.IsNumericType() && data[r, c].GetType().IsNumericType())
                            {
                                t = typeof(decimal);
                            }
                            else
                            {
                                t = typeof(string);
                                break;
                            }
                        }
                    }
                    DataColumn column = new DataColumn
                    {
                        DataType = t,
                        ColumnName = data[0, c].ToString()
                    };
                    dt.Columns.Add(column);
                }
            }
            return dt;
        }
    }
}
