using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataLoaderOptions.Readers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Extensions;
using static DataLoaderOptions.Readers.DataReader_Excel;

namespace DataLoaderOptions.Readers.Tests
{
    [TestClass()]
    public class DataReader_ExcelTests
    {
        [TestMethod()]
        public void DataSource_ExcelTest()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsx";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;

            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);

            Assert.AreEqual(path, xl.WorkbookPath);
            Assert.AreEqual(worksheetName, xl.WorksheetName);
            Assert.AreEqual(headerRow, xl.HeaderRow);
        }

        [TestMethod()]
        public void SetDataTableFormatTestRow1Header()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsx";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;

            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.SetDataTableFormat();
            DataTable tbl = xl.DataTableFormat;
            Assert.AreEqual(tbl.Columns["Date"].DataType, typeof(DateTime));
            Assert.AreEqual(tbl.Columns["ContactNo"].DataType, typeof(string));
            Assert.IsTrue(tbl.Columns["AV"].DataType.IsNumericType());
        }

        [TestMethod()]
        public void SetDataTableFormatTestPassInTable()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsx";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;
            DataTable tbl = new DataTable();
            tbl.Columns.Add("Date", typeof(DateTime));
            tbl.Columns.Add("ContactNo", typeof(string));
            tbl.Columns.Add("AV", typeof(Double));
            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.SetDataTableFormat(tbl);
        }

        [TestMethod()]
        public void LoadExcelFileTestXLSX()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsx";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;
            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.LoadExcelFile();
        }
        [TestMethod()]
        public void LoadExcelFileTestXLSB()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsb";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;
            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.LoadExcelFile();
        }
        [TestMethod()]
        public void LoadExcelFileTestXLS()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xls";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;
            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.LoadExcelFile();
        }
        [TestMethod()]
        public void LoadExcelFileTestIndex()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xls";
            int headerRow = 1;
            DataReader_Excel xl = new DataReader_Excel(path, headerRow);
            xl.LoadExcelFile();
        }
        [TestMethod()]
        public void GetDataTableTest()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsx";
            string worksheetName = "Unit Test Header Row 6";
            int headerRow = 6;

            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            DataTable tbl = xl.GetFilledDataTable(OnError.ThrowError);

            Assert.AreEqual(tbl.Columns["Date"].DataType, typeof(DateTime));
            Assert.AreEqual(tbl.Columns["ContactNo"].DataType, typeof(string));
            Assert.IsTrue(tbl.Columns["AV"].DataType.IsNumericType());
        }

        [TestMethod()]
        public void GetCellTest()
        {
            string path = "C:\\Users\\DE55\\Documents\\Working Copy\\DataLoadersC#\\DataLoaderOptionsTests\\bin\\Debug\\Test Workbook.xlsb";
            string worksheetName = "Unit Test Header Row 1";
            int headerRow = 1;
            DataReader_Excel xl = new DataReader_Excel(path, worksheetName, headerRow);
            xl.LoadExcelFile();
            Assert.AreEqual(xl.GetCell(1, 1), (object)"Date");
            Assert.AreEqual(xl.GetCell(2, 1), (object)DateTime.Parse("11-3-2017"));
        }
    }
}