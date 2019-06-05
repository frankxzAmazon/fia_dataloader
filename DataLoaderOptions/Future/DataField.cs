using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataLoaderOptions
{
    class DataField
    {
        public Type DataType { get; }
        public List<Func<Object, bool>> Checks { get; } = new List<Func<Object, bool>>();
        public List<Action<Object>> Transformations { get; } = new List<Action<Object>>();
        public string SourceName { get; }
        public string SQLName { get; }

        public bool CheckValue(object value)
        {
            if(value.GetType() != DataType)
            {
                return false;
            }
            foreach( var check in Checks)
            {
                if (!check.Invoke(value))
                    return false;
            }
            return true;
        }
        public object TransformValue(Object value)
        {
            foreach(var transformation in Transformations)
            {
                transformation.Invoke(value);
            }
            return value;
        }
    }
}
