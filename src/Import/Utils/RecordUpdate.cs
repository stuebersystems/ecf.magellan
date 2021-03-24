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
using System.Linq;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class RecordUpdate
    {        
        public static async Task<bool> AchievementType(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var success = 0;
            var sql =
                "UPDATE \"Leistungsarten\" " +
                "SET \"Bezeichnung\" = @Name " +
                "WHERE " +
                "  \"Kuerzel\" = @Code";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                // Token based table with max. length of 20, code will be cut, if longer
                string code = ecfTableReader.GetValue<string>("Code");
                if (code.Length > 20) code = code.Substring(0, 20);

                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, code);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));               

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();

            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Leistungsarten] {e.Message}");
            }


            return success > 0;
        }

        public static async Task<bool> Custodian(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"Sorgeberechtigte\" " +
                "SET " +
                "  \"Nachname\" = @LastName, \"Vorname\" = @FirstName, " +
                "  \"Strasse\" = @AddressLines, \"PLZ\" = @PostalCode, " +
                "  \"Ort\" = @Locality, \"Email\" = @Email, \"TelefonPrivat\" = @HomePhoneNumber " +
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);                
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));                                
                Helper.SetParamValue(fbCommand, "@AddressLines", FbDbType.VarChar, ecfTableReader.GetValue<string>("AddressLines"));
                Helper.SetParamValue(fbCommand, "@PostalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("PostalCode"));
                Helper.SetParamValue(fbCommand, "@Locality", FbDbType.VarChar, ecfTableReader.GetValue<string>("Locality"));
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("Email"));
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("HomePhoneNumber"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Sorgeberechtigte] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> GradeValue(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"Noten\" " +
                "SET " +
                "  \"Bezeichnung\" = @Name, \"Value\" = @Value, \"Notenart\" = @GradeSystemId " +
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";
            

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                Helper.SetParamValue(fbCommand, "@Value", FbDbType.Integer, ecfTableReader.GetValue<string>("Value"));
                Helper.SetParamValue(fbCommand, "@GradeSystemId", FbDbType.SmallInt, Convert.GradeSystem(ecfTableReader.GetValue<string>("GradeSystemId")));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Noten] {e.Message}");
            }

            return success > 0;
        }


        public static async Task<bool> SchoolClass(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, string schoolClassesId)
        {
            var success = 0;
            var sql =
                "UPDATE \"Klassen\" " +
                "SET " +
                "  \"Bezeichnung\" = @Name, \"Klassenart\" = @SchoolClassTypeId, \"Notenart\" = @GradeSystemId " +                
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"GUIDExtern\" = @GUIDExtern";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.VarChar, schoolClassesId);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                Helper.SetParamValue(fbCommand, "@SchoolClassTypeId", FbDbType.SmallInt, ecfTableReader.GetValue<string>("SchoolClassTypeId"));
                Helper.SetParamValue(fbCommand, "@GradeSystemId", FbDbType.SmallInt, ecfTableReader.GetValue<string>("GradeSystemId"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Klassen] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> SchoolTerm(FbConnection fbConnection, EcfTableReader ecfTableReader, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"Zeitraeume\" " +
                "SET" +
                "  \"Art\" = @Section, \"Bezeichnung\" = @Code, " +
                "  \"Ausdruck1\" = @Name, \"Ausdruck2\" = @Name " +
                "WHERE" +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);
                Helper.SetParamValue(fbCommand, "@Section", FbDbType.VarChar, ecfTableReader.GetValue<string>("Section"));
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, ecfTableReader.GetValue<string>("Code"));
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [SchoolTerms] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> Student(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"Schueler\" " +
                "SET " +
                "  \"Anrede\" = @Salutation, \"Nachname\" = @LastName, \"Vorname\" = @FirstName, \"Geschlecht\" = @Gender, " +
                "  \"Geburtsdatum\" = @Birthdate, \"Strasse\" = @AddressLines, \"PLZ\" = @PostalCode, \"Ort\" = @Locality, " +
                "  \"EMail\" = @Email, \"TelefonPrivat\" = @HomePhoneNumber " +
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";
            
            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);
                Helper.SetParamValue(fbCommand, "@Salutation", FbDbType.VarChar, Convert.Salutation(ecfTableReader.GetValue<string>("Salutation")));
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));
                Helper.SetParamValue(fbCommand, "@Gender", FbDbType.VarChar, Convert.Gender(ecfTableReader.GetValue<string>("Gender")));
                Helper.SetParamValue(fbCommand, "@Birthdate", FbDbType.VarChar, ecfTableReader.GetValue<string>("Birthdate"));
                Helper.SetParamValue(fbCommand, "@AddressLines", FbDbType.VarChar, ecfTableReader.GetValue<string>("AddressLines"));
                Helper.SetParamValue(fbCommand, "@PostalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("PostalCode"));
                Helper.SetParamValue(fbCommand, "@Locality", FbDbType.VarChar, ecfTableReader.GetValue<string>("Locality"));                
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("Email"));
                Helper.SetParamValue(fbCommand, "@HomePhoneNumber", FbDbType.VarChar, ecfTableReader.GetValue<string>("HomePhoneNumber"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Schueler] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> StudentCustodian(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"SchuelerSorgebe\" " +
                "SET \"Verhaeltnis\" = @RelationshipType " +
                "WHERE" +
                "  \"Mandant\" = @TenantId" +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.BigInt, id);
                Helper.SetParamValue(fbCommand, "@RelationshipType", FbDbType.SmallInt, Convert.RelationShip(ecfTableReader.GetValue<string>("RelationshipType")));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [StudentCustodian] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> StudentForeignLanguage(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int studentId, int languageId)
        {
            var success = 0;
            int sqNo = int.TryParse(ecfTableReader.GetValue<string>("SequenceNo"), out sqNo) ? sqNo : -1;

            if ((sqNo >= 1) && (sqNo <= 4))
            {

                var sqlPart =
                    $"  \"Fremdsprache{sqNo}\" = @LanguageId, \"Fremdsprache{sqNo}Von\" = @FromLevel, " +
                    $"  \"Fremdsprache{sqNo}Bis\" = @ToLevel ";

                var sql =
                    "UPDATE \"Schueler\" " +
                    $"SET {sqlPart} " +
                    "WHERE" +
                    " \"Mandant\" = @TenantId AND " +
                    "  \"ID\" = @Id";

                using var fbTransaction = fbConnection.BeginTransaction();
                try
                {
                    using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                    Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                    Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, studentId);
                    Helper.SetParamValue(fbCommand, "@LanguageId", FbDbType.VarChar, languageId);
                    Helper.SetParamValue(fbCommand, "@FromLevel", FbDbType.VarChar, ecfTableReader.GetValue<string>("FromLevel"));
                    Helper.SetParamValue(fbCommand, "@ToLevel", FbDbType.VarChar, ecfTableReader.GetValue<string>("ToLevel"));

                    success = await fbCommand.ExecuteNonQueryAsync();
                    await fbTransaction.CommitAsync();
                }
                catch (Exception e)
                {
                    fbTransaction.Rollback();
                    Console.WriteLine($"[UPDATE ERROR] [Schueler] {e.Message}");
                }

                return success > 0;
            }
            else
            {
                Console.WriteLine($"[UPDATE ERROR] [Schueler] SequenceNo out of bounds");
                return false;
            }
        }

        public static async Task<bool> StudentStatus(FbConnection fbConnection, int tenantId, int id, int status)
        {
            var success = 0;
            var sql =
                "UPDATE \"Schueler\" " +
                "SET " +
                "  \"Status\" = @Status " +
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.BigInt, id);
                Helper.SetParamValue(fbCommand, "@Status", FbDbType.SmallInt, status);

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Schueler] [Status] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> StudentSubject(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int studentSubjectId, MagellanIds magellanIds)
        {
            var success = 0;

            var sql =
                "UPDATE \"SchuelerFachdaten\" " +
                "SET " +
                "  \"Lehrer\" = @TeacherId, \"Endnote1\" = @Grade1ValueId, " +
                "  \"Leistungsart\" = @Grade1AchievementTypeId, \"Bestanden\" = @Passfail " +
                "WHERE" +
                "  \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @StudentSubjectId ";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@StudentSubjectId", FbDbType.BigInt, studentSubjectId);
                Helper.SetParamValue(fbCommand, "@TeacherId", FbDbType.BigInt, magellanIds.LehrerId);
                Helper.SetParamValue(fbCommand, "@Grade1ValueId", FbDbType.BigInt, magellanIds.Endnote1Id);
                Helper.SetParamValue(fbCommand, "@Grade1AchievementTypeId", FbDbType.VarChar, ecfTableReader.GetValue<string>("Grade1AchievementTypeId"));
                Helper.SetParamValue(fbCommand, "@Passfail", FbDbType.VarChar, Convert.Passfail(ecfTableReader.GetValue<string>("Passfail")));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                await fbTransaction.RollbackAsync();
                Console.WriteLine($"[UPDATE ERROR] [SchuelerFachdaten] {e.Message}");
            }

            return success > 0;
        }


        public static async Task<bool> Subject(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"Faecher\" " +
                "SET " +
                "  \"Bezeichnung\" = @Name " +
                "WHERE" +
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                
                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [Faecher] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> Teacher(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int id)
        {
            var success = 0;
            var sql =
                "UPDATE \"tblLehrer\" " +
                "SET " +
                "  \"Anrede\" = @Salutation, \"Nachname\" = @LastName, \"Vorname\" = @FirstName, " +
                "  \"Geschlecht\" = @Gender, \"Geburtsdatum\" = @Birthdate, \"Strasse\" = @AddressLines, " +
                "  \"PLZ\" = @PostalCode, \"Ort\" = @Locality, \"Email\" = @Email " +                
                "WHERE" + 
                " \"Mandant\" = @TenantId AND " +
                "  \"ID\" = @Id";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.VarChar, tenantId);
                Helper.SetParamValue(fbCommand, "@Id", FbDbType.VarChar, id);
                Helper.SetParamValue(fbCommand, "@Salutation", FbDbType.VarChar, Convert.Salutation(ecfTableReader.GetValue<string>("Salutation")));
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));
                Helper.SetParamValue(fbCommand, "@Gender", FbDbType.VarChar, Convert.Gender(ecfTableReader.GetValue<string>("Gender")));
                Helper.SetParamValue(fbCommand, "@Birthdate", FbDbType.VarChar, ecfTableReader.GetValue<string>("Birthdate"));
                Helper.SetParamValue(fbCommand, "@AddressLines", FbDbType.VarChar, ecfTableReader.GetValue<string>("AddressLines"));
                Helper.SetParamValue(fbCommand, "@PostalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("PostalCode"));
                Helper.SetParamValue(fbCommand, "@Locality", FbDbType.VarChar, ecfTableReader.GetValue<string>("Locality"));
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("Email"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [tblLehrer] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> TokenCatalog(FbConnection fbConnection, EcfTableReader ecfTableReader, string tableName)
        {
            var success = 0;
            var sql =
                $"UPDATE \"{tableName}\" " +
                "SET \"Schluessel\" = @InternalCode,  \"Bezeichnung\" = @Name " +
                "WHERE " +
                "  \"Kuerzel\" = @Code";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                // Token based table with max. length of 20, code will be cut, if longer                
                string code = ecfTableReader.GetValue<string>("Code");
                if (code.Length > 20) code = code.Substring(0, 20);

                string InternalCode = String.Empty;
                if (ecfTableReader.Headers.Contains("InternalCode"))
                    InternalCode = ecfTableReader.GetValue<string>("InternalCode");

                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, code);
                Helper.SetParamValue(fbCommand, "@InternalCode", FbDbType.VarChar, InternalCode);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[UPDATE ERROR] [{tableName}] {e.Message}");
            }

            return success > 0;
        }

    }
}
