#region ENBREA - Copyright (C) 2021 STÜBER SYSTEMS GmbH
/*    
 *    ENBREA 
 *    
 *    Copyright (C) 2021 STÜBER SYSTEMS GmbH
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
#endregion

using Enbrea.Ecf;
using System;
using System.Data.Common;

namespace Ecf.Magellan
{
    /// <summary>
    /// Extensions for <see cref="DbDataReader"/>
    /// </summary>
    public static class DbDataReaderExtensions
    {
        public static EcfGender? GetGender(this DbDataReader dbDataReader, string name)
        {
            var value = dbDataReader[name];
            if (value != null)
            {
                if (value.GetType() == typeof(string))
                {
                    return ((string)value) switch
                    {
                        "M" => EcfGender.Male,
                        "W" => EcfGender.Female,
                        _ => null,
                    };
                }
            }
            return null;
        }

        public static string GetSalutation(this DbDataReader dbDataReader, string name)
        {
            var value = dbDataReader[name];
            if (value != null)
            {
                if (value.GetType() == typeof(string))
                {
                    return ((string)value) switch
                    {
                        "0" => "Frau",
                        "1" => "Herr",
                        "2" => "Frau Dr.",
                        "3" => "Herr Dr.",
                        "4" => "Frau Prof.",
                        "5" => "Herr Prof.",
                        "6" => "Frau Prof.Dr.",
                        "7" => "Herr Prof.Dr.",
                        ":" => "Ms.",
                        ";" => "Mrs.",
                        "<" => "Mr.",
                        _ => null,
                    };
                }
            }
            return null;
        }

        public static bool HasColumn(this DbDataReader dbDataReader, string columnName)
        {
            for (var i = 0; i < dbDataReader.FieldCount; i++)
            {
                if (dbDataReader.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }
    }
}