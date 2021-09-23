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
using Enbrea.GuidFactory;
using FirebirdSql.Data.FirebirdClient;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class RecordInsert
    {
        public static async Task<bool> AchievementType(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var success = 0;
            var sql =
                "INSERT INTO \"Leistungsarten\" " +
                "( \"Kuerzel\", \"Bezeichnung\", \"Art\" )" +
                "VALUES ( @Code, @Name, @Art )";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                // Token based table with max. length of 20, code will be cut, if longer
                string code = ecfTableReader.GetValue<string>("Code");
                if (code.Length > 20) code = code.Substring(0, 20);

                string category = String.Empty;
                if (ecfTableReader.Headers.Contains("Category"))
                    category = ecfTableReader.GetValue<string>("Category");




                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, code);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                Helper.SetParamValue(fbCommand, "@Art", FbDbType.SmallInt, category, 0);

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();

            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Leistungsarten] {e.Message}");
            }


            return success > 0;
        }

        public static async Task<bool> Custodian(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId)
        {
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, ecfTableReader.GetValue<string>("Id"));
            var success = 0;
            var sql =
                "INSERT INTO \"Sorgeberechtigte\" " +
                "(" +
                "  \"Mandant\", \"GUIDExtern\", \"Nachname\", \"Vorname\", \"Anrede\", " +
                "  \"Strasse\", \"Land\", \"PLZ\", \"Ort\", \"Email\", \"TelefonPrivat\", \"TelefonBeruf\", " +                
                "  \"Status2\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @GUIDExtern, @LastName, @FirstName, @Salutation, " +
                "  @AddressLines, @Country, @PostalCode, @Locality, @Email, @HomePhoneNumber, @OfficePhoneNumber, " +
                "  @Status2 " +
                ")";
            

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.Integer, tenantId);
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.VarChar, guidExtern);                
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));
                Helper.SetParamValue(fbCommand, "@Salutation", FbDbType.VarChar, ValueConvert.Salutation(ecfTableReader.GetValue<string>("Salutation")));
                Helper.SetParamValue(fbCommand, "@AddressLines", FbDbType.VarChar, ecfTableReader.GetValue<string>("AddressLines"));
                Helper.SetParamValue(fbCommand, "@Country", FbDbType.VarChar, ecfTableReader.GetValue<string>("CountryId"));
                Helper.SetParamValue(fbCommand, "@PostalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("PostalCode"));
                Helper.SetParamValue(fbCommand, "@Locality", FbDbType.VarChar, ecfTableReader.GetValue<string>("Locality"));
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("Email"));
                Helper.SetParamValue(fbCommand, "@HomePhoneNumber", FbDbType.VarChar, ecfTableReader.GetValue<string>("HomePhoneNumber"));
                Helper.SetParamValue(fbCommand, "@OfficePhoneNumber", FbDbType.VarChar, ecfTableReader.GetValue<string>("OfficePhoneNumber"));
                Helper.SetParamValue(fbCommand, "@Status2", FbDbType.SmallInt, StatusType.Active);


                // Status

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Sorgeberechtigte] {e.Message}");
            }


            return success > 0;
        }

        public static async Task<bool> GradeValue(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId)
        {
            var success = 0;
            var sql =
                $"INSERT INTO \"Noten\" " +
                "( \"Mandant\", \"Notenkuerzel\", \"Bezeichnung\", \"Notenart\", \"Notenwert\" )" +
                "VALUES ( @TenantId, @Code, @Name, @GradeSystemId, @Value )";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                // Token based table with max. length of 20, code will be cut, if longer
                string code = ecfTableReader.GetValue<string>("Code");
                if (code.Length > 20) code = code.Substring(0, 20);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, code);
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                Helper.SetParamValue(fbCommand, "@GradeSystemId", FbDbType.SmallInt, ValueConvert.GradeSystem(ecfTableReader.GetValue<string>("GradeSystemId")));
                Helper.SetParamValue(fbCommand, "@Value", FbDbType.Integer, ecfTableReader.GetValue<string>("Value"));


                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Noten] {e.Message}");
            }

            return success > 0;
        }


        public static async Task<DbResult> SchoolClass(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId)
        {
            var id = -1;
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, ecfTableReader.GetValue<string>("FederationId"));

            var sql =
                "INSERT INTO \"Klassen\" " +
                "(" +
                "  \"Mandant\", \"Kuerzel\", \"KuerzelStatistik\", \"Langname1\", \"Langname2\", " +
                "  \"GUIDExtern\", \"Schulform\", \"Klassenart\", \"Notenart\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @Code, @StatisticalCode, @Name1, @Name2, " +
                "  @GUIDExtern, @SchoolCategoryId, @SchoolClassTypeId, @GradeSystemId" +
                ") RETURNING ID";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, ecfTableReader.GetValue<string>("Code"));
                Helper.SetParamValue(fbCommand, "@StatisticalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("StatisticalCode"));
                Helper.SetParamValue(fbCommand, "@Name1", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name1"));
                Helper.SetParamValue(fbCommand, "@Name2", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name2"));
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.Guid, guidExtern);
                Helper.SetParamValue(fbCommand, "@SchoolCategoryId", FbDbType.VarChar, ecfTableReader.GetValue<string>("SchoolCategoryId"));
                Helper.SetParamValue(fbCommand, "@SchoolClassTypeId", FbDbType.SmallInt, ecfTableReader.GetValue<string>("SchoolClassTypeId"));
                Helper.SetParamValue(fbCommand, "@GradeSystemId", FbDbType.SmallInt, ValueConvert.GradeSystem(ecfTableReader.GetValue<string>("GradeSystemId")));
                FbParameter IdParam = fbCommand.Parameters.Add("@ClassId", FbDbType.Integer, Int32.MaxValue, "ID");
                IdParam.Direction = ParameterDirection.Output;

                id = (int)await fbCommand.ExecuteScalarAsync();                
                await fbTransaction.CommitAsync();

                return new DbResult(true, id);
            }
            catch (Exception e)
            {
                await fbTransaction.RollbackAsync();
                Console.WriteLine($"[INSERT ERROR] [Klassen] {e.Message}");
                return new DbResult(false, id);
            }            
        }

        public static async Task<bool> SchoolClassTerm(FbConnection fbConnection, int tenantId, int classId, int termId, string classTermId,
            EcfTableReader ecfTableReader)
        {
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, classTermId);

            var success = 0;
            var sql =
                "INSERT INTO \"KlassenZeitraeume\" " +
                "(" +
                "  \"Mandant\", \"Klasse\", \"Zeitraum\", \"GUIDExtern\", " +
                "  \"Jahrgang\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @ClassId, @TermId, @GUIDExtern, @SchoolClassYear " +                
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@ClassId",  FbDbType.BigInt, classId);
                Helper.SetParamValue(fbCommand, "@TermId",   FbDbType.BigInt, termId);
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.Guid, guidExtern);
                Helper.SetParamValue(fbCommand, "@SchoolClassYear", FbDbType.SmallInt, ecfTableReader.GetValue<string>("SchoolClassYear"));                

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();

            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [KlassenZeitraeume] {e.Message}");
            }


            return success > 0;
        }

        public static async Task<DbResult> SchoolTerm(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var section = ecfTableReader.GetValue<string>("Section") switch
            {
                "1" => 0,
                "2" => 1,
                "3" => 2,
                _ => -1,
            };

            var id = -1;            
            var sql =
                "INSERT INTO \"Zeitraeume\" " +
                "(" +
                "  \"Von\", \"Bis\", \"Art\",  " +
                "  \"Bezeichnung\", \"Ausdruck1\", \"Ausdruck2\"  " +
                "  )" +
                "VALUES ( " +
                "  @ValidFrom, @ValidTo, @Section,  " +
                "  @Code, @Name, @Name" +
                ") RETURNING ID";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@ValidFrom", FbDbType.Date, ecfTableReader.GetValue<string>("ValidFrom"));
                Helper.SetParamValue(fbCommand, "@ValidTo", FbDbType.Date, ecfTableReader.GetValue<string>("ValidTo"));
                Helper.SetParamValue(fbCommand, "@Section", FbDbType.SmallInt, section);
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, ecfTableReader.GetValue<string>("Code"));
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                FbParameter IdParam = fbCommand.Parameters.Add("@Id", FbDbType.Integer, Int32.MaxValue, "ID");
                IdParam.Direction = ParameterDirection.Output;

                id = (int)await fbCommand.ExecuteScalarAsync();
                await fbTransaction.CommitAsync();

                return new DbResult(true, id);
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Zeitraeume] {e.Message}");
                return new DbResult(false, id);
            }
        }

        public static async Task<bool> SchuelerKlassen(FbConnection fbConnection, int tenantId, int schuelerZeitraumId, string ecfStatus)
        {
            var wiederholer = ecfStatus == "W" ? "J" : String.Empty;

            var success = 0;
            var sql =
                "INSERT INTO \"SchuelerKlassen\" " +
                "(" +
                "  \"Mandant\", \"SchuelerZeitraumID\", \"Wiederholer\", \"Hauptklasse\", " +
                "  \"Ueberspringer\", \"AufnahmepruefungBestanden\", \"NachpruefungBestanden\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @SchuelerZeitraumID, @Wiederholer, @Hauptklasse, " +
                "  @Ueberspringer, @AufnahmepruefungBestanden, @NachpruefungBestanden " +
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@SchuelerZeitraumID", FbDbType.BigInt, schuelerZeitraumId);
                Helper.SetParamValue(fbCommand, "@Wiederholer", FbDbType.VarChar, wiederholer);
                Helper.SetParamValue(fbCommand, "@Hauptklasse", FbDbType.VarChar, "J");
                Helper.SetParamValue(fbCommand, "@Ueberspringer", FbDbType.VarChar, "N");
                Helper.SetParamValue(fbCommand, "@AufnahmepruefungBestanden", FbDbType.VarChar, "N");
                Helper.SetParamValue(fbCommand, "@NachpruefungBestanden", FbDbType.VarChar, "N");

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [SchuelerKlassen] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<DbResult> Student(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId)
        {
            Guid guidExtern = GuidFactory.Create(GuidFactory.DnsNamespace, ecfTableReader.GetValue<string>("Id"));

            var id = -1;
            var sql =
                "INSERT INTO \"Schueler\" " +
                "(" +
                "  \"Mandant\", \"GUIDExtern\", \"Status\", \"Anrede\", \"Nachname\", \"Vorname\", \"Geschlecht\", " +
                "  \"Geburtsdatum\", \"Strasse\", \"PLZ\", \"Ort\", \"Gemeinde\", \"EMail\", \"Telefon\", \"Mobil\", " +
                "  \"Staatsangeh1\", \"Staatsangeh2\", \"Verkehrssprache\", \"Konfession\", \"Krankenkasse\", \"ZugangAm\", " +
                "  \"Grundschuleintritt\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @GUIDExtern, @Status, @Salutation, @LastName, @FirstName, @Gender, " +
                "  @Birthdate, @AddressLines, @PostalCode, @Locality, @Region, @Email, @HomePhoneNumber, @MobileNumber, " +
                "  @Nationality1, @Nationality2, @NativeLanguage, @Religion, @HealthInsuranceProvider, @EntryDate, " +
                "  @FirstEntryDate " +
                ") RETURNING ID";
            

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.Integer, tenantId);
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.VarChar, guidExtern);
                Helper.SetParamValue(fbCommand, "@Status", FbDbType.SmallInt, 2);
                Helper.SetParamValue(fbCommand, "@Salutation", FbDbType.VarChar, ValueConvert.Salutation(ecfTableReader.GetValue<string>("Salutation")));
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));
                Helper.SetParamValue(fbCommand, "@Gender", FbDbType.VarChar, ValueConvert.Gender(ecfTableReader.GetValue<string>("Gender")));
                Helper.SetParamValue(fbCommand, "@Birthdate", FbDbType.Date, ecfTableReader.GetValue<string>("Birthdate"));
                Helper.SetParamValue(fbCommand, "@AddressLines", FbDbType.VarChar, ecfTableReader.GetValue<string>("AddressLines"));
                Helper.SetParamValue(fbCommand, "@PostalCode", FbDbType.VarChar, ecfTableReader.GetValue<string>("PostalCode"));
                Helper.SetParamValue(fbCommand, "@Locality", FbDbType.VarChar, ecfTableReader.GetValue<string>("Locality"));
                Helper.SetParamValue(fbCommand, "@Region", FbDbType.VarChar, ecfTableReader.GetValue<string>("RegionId"));
                Helper.SetParamValue(fbCommand, "@Email", FbDbType.VarChar, ecfTableReader.GetValue<string>("Email"));
                Helper.SetParamValue(fbCommand, "@HomePhoneNumber", FbDbType.VarChar, ecfTableReader.GetValue<string>("HomePhoneNumber"));
                Helper.SetParamValue(fbCommand, "@MobileNumber", FbDbType.VarChar, ecfTableReader.GetValue<string>("MobileNumber"));
                Helper.SetParamValue(fbCommand, "@Nationality1", FbDbType.VarChar, ecfTableReader.GetValue<string>("Nationality1Id"));
                Helper.SetParamValue(fbCommand, "@Nationality2", FbDbType.VarChar, ecfTableReader.GetValue<string>("Nationality2Id"));
                Helper.SetParamValue(fbCommand, "@NativeLanguage", FbDbType.VarChar, ecfTableReader.GetValue<string>("NativeLanguage"));
                Helper.SetParamValue(fbCommand, "@Religion", FbDbType.VarChar, ecfTableReader.GetValue<string>("ReligionId"));
                Helper.SetParamValue(fbCommand, "@HealthInsuranceProvider", FbDbType.VarChar, ecfTableReader.GetValue<string>("HealthInsuranceProvider"));
                Helper.SetParamValue(fbCommand, "@EntryDate", FbDbType.Date, ecfTableReader.GetValue<string>("EntryDate"));
                Helper.SetParamValue(fbCommand, "@FirstEntryDate", FbDbType.Date, ecfTableReader.GetValue<string>("FirstEntryDate"));

                FbParameter IdParam = fbCommand.Parameters.Add("@Id", FbDbType.Integer, Int32.MaxValue, "ID");
                IdParam.Direction = ParameterDirection.Output;

                id = (int)await fbCommand.ExecuteScalarAsync();
                await fbTransaction.CommitAsync();

                return new DbResult(true, id);
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Schueler] {e.Message}");
                return new DbResult(false, id);
            }            
        }

        public static async Task<bool> StudentCustodian(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int studentId, int custodianId)
        {
            var success = 0;
            var sql =
                "INSERT INTO \"SchuelerSorgebe\" " +
                "(" +
                "  \"Mandant\", \"Schueler\", \"Sorgebe\", \"Verhaeltnis\", " +                
                "  \"Benachrichtigung\", \"TelefonPrioritaet\", \"Position\" " +
                "  )" +
                "VALUES ( " +
                "  @TenantId, @StudentId, @CustodianId, @RelationshipType, @CustodianNotification, @ContactPriority, @Order " +                
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@StudentId", FbDbType.BigInt, studentId);
                Helper.SetParamValue(fbCommand, "@CustodianId", FbDbType.BigInt, custodianId);
                Helper.SetParamValue(fbCommand, "@RelationshipType", FbDbType.SmallInt, ValueConvert.RelationShip(ecfTableReader.GetValue<string>("RelationshipType")));
                Helper.SetParamValue(fbCommand, "@CustodianNotification", FbDbType.SmallInt, ValueConvert.Notification(ecfTableReader.GetValue<string>("CustodianNotification")));
                Helper.SetParamValue(fbCommand, "@ContactPriority", FbDbType.SmallInt, ValueConvert.Priority(ecfTableReader.GetValue<string>("ContactPriority")));
                Helper.SetParamValue(fbCommand, "@Order", FbDbType.SmallInt, ecfTableReader.GetValue<string>("Order"));

                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [SchuelerSorgebe] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<DbResult> StudentSchoolClassAttendance(FbConnection fbConnection, int tenantId, int studentId, Career career)
        {            
            var id = -1;
            
            var sql =
                "INSERT INTO \"SchuelerZeitraeume\" " +
                "(" +
                "  \"Mandant\", \"KlassenZeitraumID\", \"Schueler\", " +
                "  \"Klasse\", \"Zeitraum\", \"Gewechselt\", " +
                "  \"TeilnahmeZusatzangebot\", \"SportBefreit\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @ClassTermId, @StudentId, " +
                "  @SchoolClassId, @SchoolTermId, @Gewechselt, " +
                "  @TeilnahmeZusatzangebot, @SportBefreit " +
                ") RETURNING ID";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@ClassTermId", FbDbType.BigInt, career.MagellanValues.ClassTermId);
                Helper.SetParamValue(fbCommand, "@StudentId", FbDbType.BigInt, studentId);
                Helper.SetParamValue(fbCommand, "@SchoolClassId", FbDbType.BigInt, career.MagellanValues.SchoolClassId);
                Helper.SetParamValue(fbCommand, "@SchoolTermId", FbDbType.BigInt, career.MagellanValues.SchoolTermId);
                Helper.SetParamValue(fbCommand, "@Gewechselt", FbDbType.VarChar, career.MagellanValues.Gewechselt);
                Helper.SetParamValue(fbCommand, "@TeilnahmeZusatzangebot", FbDbType.VarChar, "N");
                Helper.SetParamValue(fbCommand, "@SportBefreit", FbDbType.VarChar, "N");

                FbParameter IdParam = fbCommand.Parameters.Add("@Id", FbDbType.Integer, Int32.MaxValue, "ID");
                IdParam.Direction = ParameterDirection.Output;

                id = (int)await fbCommand.ExecuteScalarAsync();
                await fbTransaction.CommitAsync();

                return new DbResult(true, id);
            }
            catch (Exception e)
            {
                await fbTransaction.RollbackAsync();
                Console.WriteLine($"[INSERT ERROR] [SchuelerZeitraeume] {e.Message}");
                return new DbResult(false, id);
            }            
        }

        public static async Task<bool> StudentSubject(FbConnection fbConnection, int tenantId, int studentId, Career career, StudentSubjects studentSubject)
        {
            var success = 0;

            var sql =
                "INSERT INTO \"SchuelerFachdaten\" " +
                "(" +
                "  \"Mandant\", \"SchuelerZeitraumID\", \"Schueler\", " +
                "  \"Klasse\", \"Zeitraum\", \"Fach\", \"Lehrer\", " +
                "  \"KursNr\", \"Unterrichtsart\", \"Endnote1\", " +
                "  \"Leistungsart\", \"Bestanden\" " +
                ") " +
                "VALUES ( " +
                "  @TenantId, @StudentTermId, @StudentId, " +
                "  @SchoolClassId, @SchoolTermId, @SubjectId, @TeacherId, " +
                "  @CourseNo, @CourseTypeId, @Grade1ValueId, " +
                "  @Grade1AchievementTypeId, @Passfail " +
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@StudentTermId", FbDbType.BigInt, career.MagellanValues.StudentTermId);
                Helper.SetParamValue(fbCommand, "@StudentId", FbDbType.BigInt, studentId);
                Helper.SetParamValue(fbCommand, "@SchoolClassId", FbDbType.BigInt, career.MagellanValues.SchoolClassId);
                Helper.SetParamValue(fbCommand, "@SchoolTermId", FbDbType.BigInt, career.MagellanValues.SchoolTermId);
                Helper.SetParamValue(fbCommand, "@SubjectId", FbDbType.BigInt, studentSubject.MagellanValues.SubjectId);
                Helper.SetParamValue(fbCommand, "@TeacherId", FbDbType.BigInt, studentSubject.MagellanValues.TeacherId);
                Helper.SetParamValue(fbCommand, "@CourseNo", FbDbType.SmallInt, studentSubject.EcfValues.CourseNo);
                Helper.SetParamValue(fbCommand, "@CourseTypeId", FbDbType.SmallInt, studentSubject.EcfValues.CourseTypeId);
                Helper.SetParamValue(fbCommand, "@Grade1ValueId", FbDbType.BigInt, studentSubject.MagellanValues.Grade1ValueId);
                Helper.SetParamValue(fbCommand, "@Grade1AchievementTypeId", FbDbType.VarChar, studentSubject.EcfValues.Grade1AchievementTypeId);
                Helper.SetParamValue(fbCommand, "@Passfail", FbDbType.VarChar, ValueConvert.Passfail(studentSubject.EcfValues.Passfail));


                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();                
            }
            catch (Exception e)
            {
                await fbTransaction.RollbackAsync();
                Console.WriteLine($"[INSERT ERROR] [SchuelerFachdaten] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> Subject(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, object category = null)
        {
            var success = 0;
            var sql =
                "INSERT INTO \"Faecher\" " +
                "(" +
                "  \"Mandant\", \"Kuerzel\", \"Bezeichnung\", \"Kategorie\" " +                
                ") " +
                "VALUES ( " +
                "  @TenantId, @Code, @Name, @Category " +                
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.Integer, tenantId);
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, ecfTableReader.GetValue<string>("Code"));
                Helper.SetParamValue(fbCommand, "@Name", FbDbType.VarChar, ecfTableReader.GetValue<string>("Name"));
                Helper.SetParamValue(fbCommand, "@Category", FbDbType.SmallInt, category);


                success = await fbCommand.ExecuteNonQueryAsync();
                await fbTransaction.CommitAsync();
            }
            catch (Exception e)
            {
                fbTransaction.Rollback();
                Console.WriteLine($"[INSERT ERROR] [Faecher] {e.Message}");
            }

            return success > 0;
        }

        public static async Task<bool> Teacher(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId)
        {
            var success = 0;
            var sql =
                "INSERT INTO \"tblLehrer\" " +
                "(" +
                "  \"Mandant\", \"GUIDExtern\", \"Kuerzel\", \"Anrede\", \"Nachname\", \"Vorname\", " +
                "  \"Geschlecht\", \"Geburtsdatum\", \"Strasse\", \"PLZ\", \"Ort\", \"Email\" " +                
                ") " +
                "VALUES ( " +
                "  @TenantId, @GUIDExtern, @Code, @Salutation, @LastName, @FirstName, @Gender, " +
                "  @Birthdate, @AddressLines, @PostalCode, @Locality, @Email " +
                ")";

            using var fbTransaction = fbConnection.BeginTransaction();
            try
            {
                using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

                Helper.SetParamValue(fbCommand, "@TenantId", FbDbType.BigInt, tenantId);
                Helper.SetParamValue(fbCommand, "@GUIDExtern", FbDbType.BigInt, ecfTableReader.GetValue<string>("Id"));
                Helper.SetParamValue(fbCommand, "@Code", FbDbType.VarChar, ecfTableReader.GetValue<string>("Code"));
                Helper.SetParamValue(fbCommand, "@Salutation", FbDbType.VarChar, ValueConvert.Salutation(ecfTableReader.GetValue<string>("Salutation")));
                Helper.SetParamValue(fbCommand, "@LastName", FbDbType.VarChar, ecfTableReader.GetValue<string>("LastName"));
                Helper.SetParamValue(fbCommand, "@FirstName", FbDbType.VarChar, ecfTableReader.GetValue<string>("FirstName"));
                Helper.SetParamValue(fbCommand, "@Gender", FbDbType.VarChar, ValueConvert.Gender(ecfTableReader.GetValue<string>("Gender")));
                Helper.SetParamValue(fbCommand, "@Birthdate", FbDbType.Date, ecfTableReader.GetValue<string>("Birthdate"));
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
                Console.WriteLine($"[INSERT ERROR] [Lehrer] {e.Message}");
            }


            return success > 0;
        }

        public static async Task<bool> TokenCatalog(FbConnection fbConnection, EcfTableReader ecfTableReader, string tableName)
        {
            var success = 0;
            var sql =
                $"INSERT INTO \"{tableName}\" " +
                "( \"Kuerzel\", \"Schluessel\", \"Bezeichnung\" )" +
                "VALUES ( @Code, @InternalCode, @Name )";

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
                Console.WriteLine($"[INSERT ERROR] [{tableName}] {e.Message}");
            }

            return success > 0;
        }
    }
}
