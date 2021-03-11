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

using FirebirdSql.Data.FirebirdClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class RecordExists
    {
        public static async Task<int> SchoolTerm(FbConnection fbConnection, string validFrom, string validTo)
        {
            string sql =
                "SELECT \"ID\" FROM \"Zeitraeume\" " +
                "WHERE " +
                "  \"Von\" = :validFrom and " +
                "  \"Bis\" = :validTo";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.AddWithValue("validFrom", validFrom);
            fbCommand.Parameters.AddWithValue("validTo", validTo);

            int numberOfRecords = Convert.ToInt32(await fbCommand.ExecuteReaderAsync());
            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }
            
            if (numberOfRecords > 1) id = -1;
                        
            return id;
        }            
    }
}
