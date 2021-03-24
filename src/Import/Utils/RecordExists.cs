﻿#region ENBREA - Copyright (C) 2021 STÜBER SYSTEMS GmbH
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
using Enbrea.GuidFactory;
using FirebirdSql.Data.FirebirdClient;
using System;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class RecordExists
    {
        public static async Task<DbResult> ByCode(FbConnection fbConnection, string tableName, string code)
        {
            string sql =
                $"SELECT \"ID\" FROM \"{tableName}\" " +
                "WHERE \"Kuerzel\" = @code";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("code", code);            

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }
            
            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, -1); ;
        }

        public static async Task<DbResult> ByCodeAndTenant(FbConnection fbConnection, string tableName, int tenantId, string code)
        {
            string codeColumn;

            if (tableName == MagellanTables.GradeValues)
                codeColumn = "Notenkuerzel";
            else
                codeColumn = "Kuerzel";

            string sql =
                $"SELECT \"ID\" FROM \"{tableName}\" " +
                $"WHERE " + 
                "  \"Mandant\" = @TenantId AND " +
                "  \"{codeColumn}\" = @Code";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@Code", code);

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, -1); ;
        }


        public static async Task<DbResult> ByGuidExtern(FbConnection fbConnection, string tableName, int tenantId, string guidId)
        {
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, guidId);

            string sql =
                $"SELECT \"ID\" FROM \"{tableName}\" " +
                "WHERE " + 
                "  \"Mandant\" = @TenantId and " +
                "  \"GUIDExtern\" = @GUIDExtern";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@GUIDExtern", guidExtern);

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, -1); ;
        }

        public static async Task<DbResult> SchoolClassTerm(FbConnection fbConnection, int schoolTermId, string schoolClassId)
        {
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, schoolClassId);

            string sql =
                "SELECT \"KlassenZeitraumID\" FROM \"KlassenAnsicht\" " +
                "WHERE " +
                "  \"Zeitraum\" = @SchoolTermId and " +
                "  \"GUIDExtern\" = @GUIDExtern";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("SchoolTermId", schoolTermId);
            fbCommand.Parameters.Add("GUIDExtern", guidExtern);

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things                                
                id = (int)sqlReader["KlassenZeitraumID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, -1);
        }

        public static async Task<DbResult> SchoolTerm(FbConnection fbConnection, string validFrom, string validTo)
        {      
            string sql =
                "SELECT \"ID\" FROM \"Zeitraeume\" " +
                "WHERE " +
                "  \"Von\" = @validFrom and " +
                "  \"Bis\" = @validTo";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("validFrom", validFrom);
            fbCommand.Parameters.Add("validTo", validTo);
            
            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);
                        
            return new DbResult(false, id); ;
        }

        public static async Task<DbResult> StudentCustodian(FbConnection fbConnection, int tenantId, int studentId, int custodianId)
        {
            string sql =
                "SELECT \"ID\" FROM \"SchuelerSorgebe\" " +
                "WHERE " +
                "  \"Mandant\" = @TenantId and " +
                "  \"Schueler\" = @StudentId and " +
                "  \"Sorgebe\" = @CustodianId";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@StudentId", studentId);
            fbCommand.Parameters.Add("@CustodianId", custodianId);

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, id); ;
        }

        public static async Task<DbResult> StudentSchoolClassAttendances(FbConnection fbConnection, int tenantId, int classTermId, int studentId)
        {
            string sql =
                "SELECT \"ID\" FROM \"SchuelerZeitraeume\" " +
                "WHERE " +
                "  \"Mandant\" = @TenantId and " +
                "  \"KlassenZeitraumID\" = @ClassTermId and " +
                "  \"Schueler\" = @StudentId";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@ClassTermId", classTermId);
            fbCommand.Parameters.Add("@StudentId", studentId);            

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, id); ;
        }

        public static async Task<DbResult> StudentSubject(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, MagellanIds magellanIds)
        {
            string sql =
                "SELECT \"ID\" FROM \"Fachdaten\" " +
                "WHERE " +
                "  \"Mandant\" = @TenantId AND " +
                "  \"SchuelerZeitraumID\" = @SchuelerZeitraumId AND " +
                "  \"Fach\" = @SubjectId AND " +
                "  \"Unterrichtsart\" = @CourseTypeId AND " +
                "  \"KursNr\" = @CourseNo ";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@SchuelerZeitraumId", magellanIds.SchuelerZeitraumId);
            fbCommand.Parameters.Add("@SubjectId", magellanIds.FachId);
            fbCommand.Parameters.Add("@Unterrichtsart", ecfTableReader.GetValue<string>("CourseNo"));
            fbCommand.Parameters.Add("@CourseNo", ecfTableReader.GetValue<string>("CourseTypeId"));

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, id); ;
        }


        public static async Task<DbResult> Subject(FbConnection fbConnection, int tenantId, string code)
        {
            string sql =
                $"SELECT \"ID\" FROM \"Faecher\" " +
                "WHERE " +
                "  \"Mandant\" = @TenantId and "  +
                "  \"Kuerzel\" = @Code";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@TenantId", tenantId);
            fbCommand.Parameters.Add("@Code", code);

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            var id = -1;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                // do things
                id = (int)sqlReader["ID"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) return new DbResult(true, id);

            return new DbResult(false, -1); ;
        }

        public static async Task<DbResult> TokenCatalog(FbConnection fbConnection, string tableName, string code)
        {
            // Tokens in MAGELLAN have a max length of 20 chars, if the token is bigger, it will be cut
            if (code.Length > 20) code = code.Substring(0, 20);

            string sql =
                $"SELECT \"Kuerzel\" FROM \"{tableName}\" " +
                "WHERE " +
                "  \"Kuerzel\" = @code";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("code", code);            

            var sqlReader = await fbCommand.ExecuteReaderAsync();
            code = String.Empty;
            var numberOfRecords = 0;

            while (sqlReader.Read())
            {
                code = (string)sqlReader["Kuerzel"];
                numberOfRecords++;
            }

            if (numberOfRecords == 1) new DbResult(true, code);

            return new DbResult(false, null);
        }
    }
}
