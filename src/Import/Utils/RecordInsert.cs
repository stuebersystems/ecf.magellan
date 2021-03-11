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
using FirebirdSql.Data.FirebirdClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class RecordInsert
    {
        public static async Task<bool> SchoolTerms(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var sql =
                "INSERT INTO \"Zeitraeume\" " +
                "(" +
                "  \"Von\", \"Bis\", \"Art\"  " +
                "  \"Bezeichnung\", \"Ausdruck1\", \"Ausdruck2\"  " +
                "  )" +
                "VALUES ( " +
                "  \"Von\" = :ValidFrom, \"Bis\" = :ValidTo, \"Art\" = :Section,  " +
                "  \"Bezeichnung\" = :Code, \"Ausdruck1\" = :Name, \"Ausdruck2\" = :Name" +
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.AddWithValue("validFrom", ecfTableReader.GetValue<string>("ValidFrom"));
            fbCommand.Parameters.AddWithValue("validTo", ecfTableReader.GetValue<string>("ValidTo"));
           
            
            var success = await fbCommand.ExecuteNonQueryAsync();

            return true;
        }
    }
}
