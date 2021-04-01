using System;
using System.Collections.Generic;
using System.Text;

namespace Ecf.Magellan
{
    public class DbResult
    {
        public bool Success { get; set; }
        
        public object Value { get; set; }

        public DbResult(bool success, object value)
        {
            Success = success;
            Value = value;
        }
    }
}
