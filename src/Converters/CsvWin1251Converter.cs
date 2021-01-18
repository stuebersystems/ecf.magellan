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

using Enbrea.Csv;
using System;

namespace Ecf.Magellan
{
    /// <summary>
    /// String converter for MAGELLAN 6 databases
    /// </summary>
    public class CsvWin1251Converter : CsvDefaultConverter
    {
        public CsvWin1251Converter() : base(typeof(object))
        {
        }

        public override string ToString(object value)
        {
            if ((value == null) || (value is DBNull))
            {
                return string.Empty;
            }
            else
            {
                /// MAGELLAN 6 databases are stored with charset WIN1251, which leads to (expected) problems reading 
                /// strings via ADO.NET. This converter maps WIN1251 characters to WIN1250 characters to fix this. 
                /// MAGELLAN 7 does not have this problem since MAGELLAN 7 database are always stored with charset 
                /// UTF-8.
                if (value is string strValue)
                {
                    strValue = strValue.Replace('ь', 'ü');
                    strValue = strValue.Replace('Ь', 'Ü');
                    strValue = strValue.Replace('д', 'ä');
                    strValue = strValue.Replace('Д', 'Ä'); 
                    strValue = strValue.Replace('ц', 'ö');
                    strValue = strValue.Replace('Ц', 'Ö');
                    strValue = strValue.Replace('Я', 'ß');
                    strValue = strValue.Replace('б', 'á');
                    strValue = strValue.Replace('в', 'â');
                    strValue = strValue.Replace('г', 'ă');
                    strValue = strValue.Replace('е', 'ĺ');
                    strValue = strValue.Replace('ж', 'ć');
                    strValue = strValue.Replace('з', 'ç');
                    strValue = strValue.Replace('и', 'č');
                    strValue = strValue.Replace('й', 'é');
                    strValue = strValue.Replace('м', 'ě');
                    strValue = strValue.Replace('н', 'í');
                    strValue = strValue.Replace('р', 'đ');
                    strValue = strValue.Replace('с', 'ń');
                    strValue = strValue.Replace('т', 'ň');
                    strValue = strValue.Replace('у', 'ó');
                    strValue = strValue.Replace('ф', 'ô');
                    strValue = strValue.Replace('х', 'ő');
                    strValue = strValue.Replace('ъ', 'ú');
                    strValue = strValue.Replace('ы', 'ű');
                    strValue = strValue.Replace('э', 'ý');

                    return strValue;

                }
                else
                {
                    return value.ToString();
                }
            }
        }
    }
}