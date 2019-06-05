using CsvHelper;
using DataLoaderOptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataLoaderOptions.RowIndicators;

namespace DataLoaderOptions.Readers
{
    class DataReader_Text_Delimited
    {
        public string Delimiter { get; set; } = ",";
        public OnError OnError { get; set; } = OnError.UseNullValue;
        public RowRules RowRules { get; set; }
        public bool HasHeaders { get; set; }
        public DataTable ExpectedFormat { get; set; }
        public DataTable GetDataTable(string path)
        {
            DataTable toReturn = ExpectedFormat.Clone();

            using (TextReader reader = new StreamReader(path))
            {
                using (ICsvParser csv = new CsvFactory().CreateParser(reader))
                {
                    csv.Configuration.Delimiter = Delimiter;
                    int rowNumber = 1;

                    while (true)
                    {
                        string[] row = csv.Read();
                        string fullRow = string.Join(Delimiter, row);
                        if (row == null || RowRules.IsLastRow(rowNumber, fullRow))
                        {
                            break;
                        }
                        else if (RowRules.ToUseRow(rowNumber, string.Join(Delimiter, fullRow)))
                        {
                            DataRow toInsert = toReturn.NewRow();

                            toReturn.Rows.Add(toInsert);
                        }
                    }
                }
            }
            return toReturn;
        }

        private DataRow FillNoheaders(DataTable format, string[] row)
        {
            int i = 0;
            DataRow toReturn = format.NewRow();
            foreach (DataColumn col in format.Columns)
            {
                if (row[i] != "" && toReturn.Table.Columns.Contains(col.ColumnName))
                {
                    toReturn[col.ColumnName] = Convert.ChangeType(row[i++], col.DataType);
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
            return toReturn;
        }
    }
}
