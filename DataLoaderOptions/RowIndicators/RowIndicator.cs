using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Extensions;
namespace DataLoaderOptions.RowIndicators
{
    public class RowRules
    {
        private bool hasStarted = false;
        public int StartRowNumber { get; set; } = 1;
        public int EndRowNumber { get; set; } = int.MaxValue;
        public string StartValue { get; set; }
        public string EndValue { get; set; }
        public bool ExactMatch { get; set; }
        public bool IsLastRow(int rowNumber, string row)
        {
            if (hasStarted)
            {
                if (ExactMatch && row.Equals(EndValue, StringComparison.InvariantCultureIgnoreCase))
                {
                    return true;
                }
                else if (row.CaseInsensitiveContains(EndValue))
                {
                    return true;
                }
            }
            return false;
        }
        public bool ToUseRow(int rowNumber, string row)
        {
            if (string.IsNullOrEmpty(row))
            {
                return false;
            }
            else if(StartRowNumber <= rowNumber && rowNumber <= EndRowNumber)
            {
                if (ExactMatch && row.Equals(EndValue, StringComparison.InvariantCultureIgnoreCase))
                {
                    return false;
                }
                else if(row.CaseInsensitiveContains(EndValue))
                {
                    return false;
                }
                else if(string.IsNullOrEmpty(StartValue))
                {
                    hasStarted = true;
                    return true;
                }
                else
                {
                    bool toRet = hasStarted;
                    if (ExactMatch && row.Equals(StartValue, StringComparison.InvariantCultureIgnoreCase))
                    {
                        hasStarted = true;
                    }
                    else if (row.CaseInsensitiveContains(StartValue))
                    {
                        hasStarted = true;
                    }
                    return toRet;
                }
            }
            return false;
        }
    }
}
