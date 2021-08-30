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
using Enbrea.Ecf;
using FirebirdSql.Data.FirebirdClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public class ExportManager : CustomManager
    {
        private readonly int _schoolTermId;
        private readonly int _tenantId;
        private int _recordCounter = 0;
        private int _tableCounter = 0;
        private int _version = 0;

        public ExportManager(
            Configuration config,
            CancellationToken cancellationToken,
            EventWaitHandle cancellationEvent)
            : base(config, cancellationToken, cancellationEvent)
        {
            _tenantId = _config.EcfExport.TenantId;
            _schoolTermId = _config.EcfExport.SchoolTermId;
        }

        public async override Task Execute()
        {
            using var connection = new FbConnection(_config.EcfExport?.DatabaseConnection);
            try
            {
                connection.Open();
                try
                {
                    _tableCounter = 0;
                    _recordCounter = 0;
                    _version = await ReadMagellanVersion(connection);

                    // Report status
                    Console.WriteLine();
                    Console.WriteLine("[Extracting] Start...");

                    // Preparation
                    PrepareExportFolder();

                    // Catalogs
                    await Execute(EcfTables.Countries, connection, async (c, w, h) => await ExportCatalog("Staaten", c, w, h));
                    await Execute(EcfTables.CourseCategories, connection, async (c, w, h) => await ExportCatalog("Fachstati", c, w, h));
                    await Execute(EcfTables.CourseTypes, connection, async (c, w, h) => await ExportCatalog("Unterrichtsarten", c, w, h));
                    await Execute(EcfTables.FormsOfTeaching, connection, async (c, w, h) => await ExportCatalog("Unterrichtsformen", c, w, h));
                    await Execute(EcfTables.Languages, connection, async (c, w, h) => await ExportCatalog("Muttersprachen", c, w, h));
                    await Execute(EcfTables.Nationalities, connection, async (c, w, h) => await ExportCatalog("Staatsangehoerigkeiten", c, w, h));
                    await Execute(EcfTables.Religions, connection, async (c, w, h) => await ExportCatalog("Konfessionen", c, w, h));
                    await Execute(EcfTables.SchoolCategories, connection, async (c, w, h) => await ExportCatalog("Schulformen", c, w, h));
                    await Execute(EcfTables.SchoolClassFlags, connection, async (c, w, h) => await ExportCatalog("KlassenMerkmale", c, w, h));
                    await Execute(EcfTables.SchoolClassLevels, connection, async (c, w, h) => await ExportCatalog("Klassenstufen", c, w, h));
                    await Execute(EcfTables.SchoolOrganisations, connection, async (c, w, h) => await ExportCatalog("Organisationen", c, w, h));
                    await Execute(EcfTables.SchoolTypes, connection, async (c, w, h) => await ExportCatalog("Schularten", c, w, h));
                    await Execute(EcfTables.SubjectLevels, connection, async (c, w, h) => await ExportCatalog("FachNiveaus", c, w, h));
                    await Execute(EcfTables.SubjectFocuses, connection, async (c, w, h) => await ExportCatalog("Fachschwerpunkte", c, w, h));
                    await Execute(EcfTables.SubjectGroups, connection, async (c, w, h) => await ExportCatalog("Fachgruppen", c, w, h));

                    // Special catalogs
                    await Execute(EcfTables.EducationalPrograms, connection, async (c, w, h) => await ExportEducationalPrograms(c, w, h));
                    await Execute(EcfTables.MaritalStatuses, connection, async (c, w, h) => await ExportMaritalStatuses(w));
                    await Execute(EcfTables.SchoolClassTypes, connection, async (c, w, h) => await ExportSchoolClassTypes(w));
                    await Execute(EcfTables.SubjectCategories, connection, async (c, w, h) => await ExportSubjectCategories(w));
                    await Execute(EcfTables.SubjectTypes, connection, async (c, w, h) => await ExportSubjectTypes(w));

                    // Education
                    await Execute(EcfTables.Departments, connection, async (c, w, h) => await ExportDepartments(c, w, h));
                    await Execute(EcfTables.Subjects, connection, async (c, w, h) => await ExportSubjects(c, w, h));
                    await Execute(EcfTables.Teachers, connection, async (c, w, h) => await ExportTeachers(c, w, h));
                    await Execute(EcfTables.SchoolClasses, connection, async (c, w, h) => await ExportSchoolClasses(c, w, h));
                    await Execute(EcfTables.Students, connection, async (c, w, h) => await ExportStudents(c, w, h));
                    await Execute(EcfTables.StudentSchoolClassAttendances, connection, async (c, w, h) => await ExportStudentSchoolClassAttendances(c, w, h));
                    await Execute(EcfTables.StudentSubjects, connection, async (c, w, h) => await ExportStudentSubjects(c, w, h));

                    // Report status
                    Console.WriteLine($"[Extracting] {_tableCounter} table(s) and {_recordCounter} record(s) extracted");
                }
                finally
                {
                    connection.Close();
                }
            }
            catch
            {
                // Report status 
                Console.WriteLine();
                Console.WriteLine($"[Error] Extracting failed. Only {_tableCounter} table(s) and {_recordCounter} record(s) extracted");
                throw;
            }
        }

        private async Task Execute(string ecfTableName, FbConnection fbConnection, Func<FbConnection, EcfTableWriter, string[], Task<int>> action)
        {
            if (ShouldExportTable(ecfTableName, out var ecfFile))
            {
                // Report status
                Console.WriteLine($"[Extracting] [{ecfTableName}] Start...");

                // Init CSV file stream
                using var ecffileStream = new FileStream(Path.ChangeExtension(Path.Combine(_config.EcfExport?.TargetFolderName, ecfTableName), "csv"), FileMode.Create, FileAccess.ReadWrite, FileShare.None);

                // Init CSV Writer
                using var csvWriter = new CsvWriter(ecffileStream, Encoding.UTF8);

                // Init ECF Writer
                var ecfTablefWriter = new EcfTableWriter(csvWriter);
                
                if (_version < 7)
                {
                    ecfTablefWriter.AddConverter<object>(new CsvWin1251Converter());
                }

                // Call table specific action
                var ecfRecordCounter = await action(fbConnection, ecfTablefWriter, ecfFile?.Headers);

                // Inc counters
                _recordCounter += ecfRecordCounter;
                _tableCounter++;

                // Report status
                Console.WriteLine($"[Extracting] [{ecfTableName}] {ecfRecordCounter} record(s) extracted");
            }
        }

        private async Task<int> ExportCatalog(string fbTableName, FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = $"select * from \"{fbTableName}\"";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.StatisticalCode,
                    EcfHeaders.InternalCode,
                    EcfHeaders.Name);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.StatisticalCode, reader["StatistikID"]);
                ecfTableWriter.SetValue(EcfHeaders.InternalCode, reader["Schluessel"]);
                ecfTableWriter.SetValue(EcfHeaders.Name, reader["Bezeichnung"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportDepartments(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = $"select * from \"Abteilungen\" where \"Mandant\" = @tenantId";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.Name);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.Name, reader["Bezeichnung"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportEducationalPrograms(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = $"select * from \"Bildungsgaenge\"";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.StatisticalCode,
                    EcfHeaders.InternalCode,
                    EcfHeaders.Name);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.StatisticalCode, reader["StatistikID"]);
                ecfTableWriter.SetValue(EcfHeaders.InternalCode, reader["Schluessel"]);
                ecfTableWriter.SetValue(EcfHeaders.Name, reader["Bezeichnung"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportSchoolClasses(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = 
                $"select K.*, L1.\"ID\" as \"Lehrer1\", L2.\"ID\" as \"Lehrer2\" from \"KlassenAnsicht\" as K " +
                $"left join \"Lehrer\" as L1 " +
                $"on K.\"Mandant\" = L1.\"Mandant\" and K.\"Klassenleiter1\" = L1.\"ID\" and L1.\"Status\" = 1 " +
                $"left join \"Lehrer\" as L2 " +
                $"on K.\"Mandant\" = L2.\"Mandant\" and K.\"Klassenleiter2\" = L2.\"ID\" and L2.\"Status\" = 1 " +
                $"where K.\"Mandant\" = @tenantId and K.\"Zeitraum\" = @schoolTermId ";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);
            fbCommand.Parameters.Add("@schoolTermId", _schoolTermId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.StatisticalCode,
                    EcfHeaders.Name1,
                    EcfHeaders.Name2,
                    EcfHeaders.SchoolClassTypeId,
                    EcfHeaders.SchoolClassLevelId,
                    EcfHeaders.DepartmentId,
                    EcfHeaders.SchoolTypeId,
                    EcfHeaders.SchoolCategoryId,
                    EcfHeaders.SchoolOrganisationId,
                    EcfHeaders.Teacher1Id,
                    EcfHeaders.Teacher2Id,
                    EcfHeaders.FormOfTeachingId);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["Id"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.StatisticalCode, reader["KuerzelStatistik"]);
                ecfTableWriter.SetValue(EcfHeaders.Name1, reader["Langname1"]);
                ecfTableWriter.SetValue(EcfHeaders.Name2, reader["Langname2"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolClassTypeId, reader["Klassenart"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolClassLevelId, reader["Klassenstufe"]);
                ecfTableWriter.SetValue(EcfHeaders.DepartmentId, reader["Abteilung"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolTypeId, reader["Schulart"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolCategoryId, reader["Schulform"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolOrganisationId, reader["Organisation"]);
                ecfTableWriter.SetValue(EcfHeaders.Teacher1Id, reader["Lehrer1"]);
                ecfTableWriter.SetValue(EcfHeaders.Teacher2Id, reader["Lehrer2"]);
                ecfTableWriter.SetValue(EcfHeaders.FormOfTeachingId, reader["Unterrichtsform"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportSchoolClassTypes(EcfTableWriter ecfTableWriter)
        {
            await ecfTableWriter.WriteHeadersAsync(
                EcfHeaders.Id,
                EcfHeaders.Code,
                EcfHeaders.StatisticalCode,
                EcfHeaders.InternalCode,
                EcfHeaders.Name);

            await ecfTableWriter.WriteAsync("0", "STANDARD", "00", "00", "Standardklasse");
            await ecfTableWriter.WriteAsync("1", "GANZTAGS", "01", "01", "Ganztagsklasse");
            await ecfTableWriter.WriteAsync("2", "KURSE", "02", "02", "Oberstufenjahrgang (Nur Kurse)");
            await ecfTableWriter.WriteAsync("3", "LK+GK", "03", "03", "Oberstufenjahrgang (LK und GK)");
            await ecfTableWriter.WriteAsync("4", "ABSCHLUSS", "04", "04", "Abschlussklasse");
            await ecfTableWriter.WriteAsync("5", "KOMBI", "05", "05", "Kombinationsklasse");
            await ecfTableWriter.WriteAsync("6", "KINDER", "06", "06", "Schulkindergarten");
            await ecfTableWriter.WriteAsync("7", "STANDARD+O", "07", "07", "Standardklasse mit Oberstufensynchronisation");

            return await Task.FromResult(8);
        }

        private async Task<int> ExportStudents(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql;

            if (_version >= 7)
            {
                sql =
                    $"select S.* from \"Schueler\" S " +
                    $"join \"SchuelerZeitraeume\" SZ " +
                    $"on S.\"ID\" = SZ.\"Schueler\" and S.\"Mandant\" = SZ.\"Mandant\" " +
                    $"where S.\"Mandant\" = @tenantId and SZ.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3) and (S.\"IDIntern\" is NULL)";
            }
            else
            {
                sql =
                    $"select S.* from \"Schueler\" S " +
                    $"join \"SchuelerZeitraeume\" SZ " +
                    $"on S.\"ID\" = SZ.\"Schueler\" and S.\"Mandant\" = SZ.\"Mandant\" " +
                    $"where S.\"Mandant\" = @tenantId and SZ.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3)";
            }

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);
            fbCommand.Parameters.Add("@schoolTermId", _schoolTermId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfCache = new HashSet<int>();
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.LastName,
                    EcfHeaders.FirstName,
                    EcfHeaders.MiddleName,
                    EcfHeaders.Birthname,
                    EcfHeaders.Salutation,
                    EcfHeaders.Gender,
                    EcfHeaders.Birthdate,
                    EcfHeaders.Birthname,
                    EcfHeaders.PlaceOfBirth,
                    //EcfHeaders.CountryOfBirthId,
                    EcfHeaders.AddressLines,
                    EcfHeaders.PostalCode,
                    EcfHeaders.Locality,
                    //EcfHeaders.CountryId,
                    EcfHeaders.HomePhoneNumber,
                    EcfHeaders.EmailAddress,
                    EcfHeaders.MobileNumber,
                    //EcfHeaders.Nationality1Id,
                    //EcfHeaders.Nationality2Id,
                    EcfHeaders.NativeLanguageId, 
                    EcfHeaders.CorrespondenceLanguageId);
                    //EcfHeaders.ReligionId); 
            }

            while (await reader.ReadAsync())
            {
                var studentId = (int)reader["ID"];

                if (!ecfCache.Contains(studentId))
                {
                    ecfTableWriter.SetValue(EcfHeaders.Id, studentId);
                    ecfTableWriter.SetValue(EcfHeaders.LastName, reader["Nachname"]);
                    ecfTableWriter.SetValue(EcfHeaders.FirstName, reader["Vorname"]);
                    ecfTableWriter.SetValue(EcfHeaders.MiddleName, reader["Vorname2"]);
                    ecfTableWriter.SetValue(EcfHeaders.Birthname, reader["Geburtsname"]);
                    ecfTableWriter.SetValue(EcfHeaders.Salutation, reader.GetSalutation("Anrede"));
                    ecfTableWriter.SetValue(EcfHeaders.Gender, reader.GetGender("Geschlecht"));
                    ecfTableWriter.SetValue(EcfHeaders.Birthdate, reader.GetDate("Geburtsdatum"));
                    ecfTableWriter.SetValue(EcfHeaders.Birthname, reader["Geburtsname"]);
                    ecfTableWriter.SetValue(EcfHeaders.PlaceOfBirth, reader["Geburtsort"]);
                    //ecfTableWriter.SetValue(EcfHeaders.CountryOfBirthId, reader["Geburtsland"]);
                    ecfTableWriter.SetValue(EcfHeaders.AddressLines, reader["Strasse"]);
                    ecfTableWriter.SetValue(EcfHeaders.PostalCode, reader["PLZ"]);
                    ecfTableWriter.SetValue(EcfHeaders.Locality, reader["Ort"]);
                    //ecfTableWriter.SetValue(EcfHeaders.CountryId, reader["Land"]);
                    ecfTableWriter.SetValue(EcfHeaders.HomePhoneNumber, reader["Telefon"]);
                    ecfTableWriter.SetValue(EcfHeaders.EmailAddress, reader["Email"]);
                    ecfTableWriter.SetValue(EcfHeaders.MobileNumber, reader["Mobil"]);
                    //ecfTableWriter.SetValue(EcfHeaders.Nationality1Id, reader["Staatsangeh1"]);
                    //ecfTableWriter.SetValue(EcfHeaders.Nationality2Id, reader["Staatsangeh2"]);
                    ecfTableWriter.SetValue(EcfHeaders.NativeLanguageId, reader["Muttersprache"]);
                    ecfTableWriter.SetValue(EcfHeaders.CorrespondenceLanguageId, reader["Verkehrssprache"]);
                    //ecfTableWriter.SetValue(EcfHeaders.ReligionId, reader["Konfession"]);

                    await ecfTableWriter.WriteAsync();

                    ecfCache.Add(studentId);
                    ecfRecordCounter++;
                }
            }
                
            return ecfRecordCounter;
        }

        private async Task<int> ExportStudentSchoolClassAttendances(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql;

            if (_version >= 7)
            {
                sql =
                    $"select Z.*, K.\"Zugang\", K.\"Abgang\" from \"SchuelerZeitraeume\" as Z " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"ID\" = K.\"SchuelerZeitraumID\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"ID\" " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3) and (S.\"IDIntern\" is NULL) " +
                    $"union all " +
                    $"select Z.*, K.\"Zugang\", K.\"Abgang\" from \"SchuelerZeitraeume\" as Z " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"ID\" = K.\"SchuelerZeitraumID\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"IDIntern\" " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3) and not (S.\"IDIntern\" is NULL)";
            }
            else
            {
                sql =
                    $"select Z.*, K.\"Zugang\", K.\"Abgang\" from \"SchuelerZeitraeume\" as Z " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"Klasse\" = K.\"Klasse\" and Z.\"Schueler\" = K.\"Schueler\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"ID\" " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3)";
            }

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);
            fbCommand.Parameters.Add("@schoolTermId", _schoolTermId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.SchoolClassId,
                    EcfHeaders.StudentId,
                    EcfHeaders.EntryDate,
                    EcfHeaders.ExitDate);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["ID"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolClassId, reader["Klasse"]);
                ecfTableWriter.SetValue(EcfHeaders.StudentId, reader["Schueler"]);
                ecfTableWriter.SetValue(EcfHeaders.EntryDate, reader.GetDate("Zugang"));
                ecfTableWriter.SetValue(EcfHeaders.ExitDate, reader.GetDate("Abgang"));

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportStudentSubjects(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql;

            if (_version >= 7)
            {
                sql =
                    $"select F.\"ID\", F.\"Klasse\", S.\"ID\" as \"Schueler\", F.\"KursNr\", F.\"Unterrichtsart\", F.\"Fachstatus\", F.\"Fach\", " +
                    $"F.\"Niveau\", F.\"Schwerpunkt\", K.\"Zugang\", K.\"Abgang\", L.\"ID\" as \"Lehrer\" from \"SchuelerFachdaten\" as F " +
                    $"join \"SchuelerZeitraeume\" as Z " +
                    $"on Z.\"Mandant\" = F.\"Mandant\" and Z.\"ID\" = F.\"SchuelerZeitraumID\" " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"ID\" = K.\"SchuelerZeitraumID\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"ID\" " +
                    $"left join \"Lehrer\" as L " +
                    $"on F.\"Mandant\" = L.\"Mandant\" and F.\"Lehrer\" = L.\"ID\" " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3) and (S.\"IDIntern\" is NULL) " +
                    $"union all " +
                    $"select F.\"ID\", F.\"Klasse\", S.\"IDIntern\" as \"Schueler\", F.\"KursNr\", F.\"Unterrichtsart\", F.\"Fachstatus\", F.\"Fach\", " +
                    $"F.\"Niveau\", F.\"Schwerpunkt\", K.\"Zugang\", K.\"Abgang\", L.\"ID\" as \"Lehrer\" from \"SchuelerFachdaten\" as F " +
                    $"join \"SchuelerZeitraeume\" as Z " +
                    $"on Z.\"Mandant\" = F.\"Mandant\" and Z.\"ID\" = F.\"SchuelerZeitraumID\" " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"ID\" = K.\"SchuelerZeitraumID\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"IDIntern\" " +
                    $"join \"Schueler\" as I " +
                    $"on I.\"Mandant\" = S.\"Mandant\" and I.\"ID\" = S.\"IDIntern\" " +
                    $"left join \"Lehrer\" as L " +
                    $"on F.\"Mandant\" = L.\"Mandant\" and F.\"Lehrer\" = L.\"ID\" and L.\"Status\" = 1 " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and I.\"Status\" in (2, 3) and not (S.\"IDIntern\" is NULL)";
            }
            else
            {
                sql =
                    $"select F.\"ID\", F.\"Klasse\", F.\"Schueler\", F.\"KursNr\", F.\"Unterrichtsart\", F.\"Fachstatus\", F.\"Fach\", " +
                    $"F.\"Niveau\", F.\"Schwerpunkt\", K.\"Zugang\", K.\"Abgang\", L.\"ID\" as \"Lehrer\" from \"SchuelerFachdaten\" as F " +
                    $"join \"SchuelerZeitraeume\" as Z " +
                    $"on Z.\"Mandant\" = F.\"Mandant\" and Z.\"Klasse\" = F.\"Klasse\" and Z.\"Schueler\" = F.\"Schueler\" and Z.\"Zeitraum\" = F.\"Zeitraum\" " +
                    $"join \"SchuelerKlassen\" as K " +
                    $"on Z.\"Mandant\" = K.\"Mandant\" and Z.\"Klasse\" = K.\"Klasse\" and Z.\"Schueler\" = K.\"Schueler\" " +
                    $"join \"Schueler\" as S " +
                    $"on Z.\"Mandant\" = S.\"Mandant\" and Z.\"Schueler\" = S.\"ID\" " +
                    $"left join \"Lehrer\" as L " +
                    $"on F.\"Mandant\" = L.\"Mandant\" and F.\"Lehrer\" = L.\"ID\" and L.\"Status\" = 1 " +
                    $"where Z.\"Mandant\" = @tenantId and Z.\"Zeitraum\" = @schoolTermId and S.\"Status\" in (2, 3)";
            }

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);
            fbCommand.Parameters.Add("@schoolTermId", _schoolTermId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.SchoolClassId,
                    EcfHeaders.StudentId,
                    EcfHeaders.CourseNo,
                    EcfHeaders.CourseTypeId,
                    EcfHeaders.CourseCategoryId,
                    EcfHeaders.SubjectId,
                    EcfHeaders.SubjectLevelId,
                    EcfHeaders.SubjectFocusId,
                    EcfHeaders.TeacherId,
                    EcfHeaders.EntryDate,
                    EcfHeaders.ExitDate);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["ID"]);
                ecfTableWriter.SetValue(EcfHeaders.SchoolClassId, reader["Klasse"]);
                ecfTableWriter.SetValue(EcfHeaders.StudentId, reader["Schueler"]);
                ecfTableWriter.SetValue(EcfHeaders.CourseNo, reader["KursNr"]);
                ecfTableWriter.SetValue(EcfHeaders.CourseTypeId, reader["Unterrichtsart"]);
                ecfTableWriter.SetValue(EcfHeaders.CourseCategoryId, reader["Fachstatus"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectId, reader["Fach"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectLevelId, reader["Niveau"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectFocusId, reader["Schwerpunkt"]);
                ecfTableWriter.SetValue(EcfHeaders.TeacherId, reader["Lehrer"]);
                ecfTableWriter.SetValue(EcfHeaders.EntryDate, reader.GetDate("Zugang"));
                ecfTableWriter.SetValue(EcfHeaders.ExitDate, reader.GetDate("Abgang"));

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportSubjectCategories(EcfTableWriter ecfTableWriter)
        {
            await ecfTableWriter.WriteHeadersAsync(
                EcfHeaders.Id,
                EcfHeaders.Code,
                EcfHeaders.StatisticalCode,
                EcfHeaders.InternalCode,
                EcfHeaders.Name);

            await ecfTableWriter.WriteAsync("0", "SLK", "00", "00", "sprachl.-lit.-künstlerisch");
            await ecfTableWriter.WriteAsync("1", "GES", "01", "01", "gesellschaftswiss.");
            await ecfTableWriter.WriteAsync("2", "MNT", "02", "02", "mathem.-nat.-technisch");
            await ecfTableWriter.WriteAsync("3", "REL", "03", "03", "Religion");
            await ecfTableWriter.WriteAsync("4", "SP", "04", "04", "Sport");

            return await Task.FromResult(5);
        }

        private async Task<int> ExportSubjects(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = $"select * from \"Faecher\" where \"Mandant\" = @tenantId";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                EcfHeaders.Id,
                EcfHeaders.Code,
                EcfHeaders.StatisticalCode,
                EcfHeaders.InternalCode,
                EcfHeaders.Name,
                EcfHeaders.SubjectTypeId,
                EcfHeaders.SubjectCategoryId,
                EcfHeaders.SubjectGroupId);
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["ID"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.StatisticalCode, reader["StatistikID"]);
                ecfTableWriter.SetValue(EcfHeaders.InternalCode, reader["Schluessel"]);
                ecfTableWriter.SetValue(EcfHeaders.Name, reader["Bezeichnung"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectTypeId, reader["Kategorie"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectCategoryId, reader["Aufgabenbereich"]);
                ecfTableWriter.SetValue(EcfHeaders.SubjectGroupId, reader["Gruppe"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportMaritalStatuses(EcfTableWriter ecfTableWriter)
        {
            await ecfTableWriter.WriteHeadersAsync(
                EcfHeaders.Id,
                EcfHeaders.Code,
                EcfHeaders.StatisticalCode,
                EcfHeaders.InternalCode,
                EcfHeaders.Name);

            await ecfTableWriter.WriteAsync("0", "VH", "00", "00", "Verheiratet");
            await ecfTableWriter.WriteAsync("1", "NV", "01", "01", "Nicht Verheiratet");
            await ecfTableWriter.WriteAsync("2", "LE", "02", "02", "Ledig");
            await ecfTableWriter.WriteAsync("3", "GE", "03", "03", "Geschieden");
            await ecfTableWriter.WriteAsync("4", "VW", "04", "04", "Verwitwet");

            return await Task.FromResult(5);
        }

        private async Task<int> ExportSubjectTypes(EcfTableWriter ecfTableWriter)
        {
            await ecfTableWriter.WriteHeadersAsync(
                EcfHeaders.Id,
                EcfHeaders.Code,
                EcfHeaders.StatisticalCode,
                EcfHeaders.InternalCode,
                EcfHeaders.Name);

            await ecfTableWriter.WriteAsync("0", "FS", "00", "00", "Fremdsprache");
            await ecfTableWriter.WriteAsync("1", "REL", "01", "01", "Religion / Ethik");
            await ecfTableWriter.WriteAsync("2", "DEU", "02", "02", "Deutsch");
            await ecfTableWriter.WriteAsync("3", "MAT", "03", "03", "Mathematik");
            await ecfTableWriter.WriteAsync("4", "KUN", "04", "04", "Kunst");
            await ecfTableWriter.WriteAsync("5", "MUS", "05", "05", "Musik");
            await ecfTableWriter.WriteAsync("6", "SP", "06", "06", "Sport");
            await ecfTableWriter.WriteAsync("9", "INF", "09", "09", "Informatik");
            await ecfTableWriter.WriteAsync("10", "PHI", "10", "10", "Philosophie");
            await ecfTableWriter.WriteAsync("11", "GES", "11", "11", "Geschichte");
            await ecfTableWriter.WriteAsync("12", "PHY", "12", "12", "Physik");
            await ecfTableWriter.WriteAsync("13", "CHE", "13", "13", "Chemie");
            await ecfTableWriter.WriteAsync("14", "BIO", "14", "14", "Biologie");
            await ecfTableWriter.WriteAsync("15", "ERD", "15", "15", "Erdkunde");
            await ecfTableWriter.WriteAsync("16", "SOZ", "16", "16", "Sozialkunde");
            await ecfTableWriter.WriteAsync("17", "WIR", "17", "17", "Wirtschaft");
            await ecfTableWriter.WriteAsync("18", "POL", "18", "18", "Politik");
            await ecfTableWriter.WriteAsync("19", "DSP", "19", "19", "Darstellendes Spiel");
            await ecfTableWriter.WriteAsync("20", "EREL", "20", "20", "Evangelische Religion");
            await ecfTableWriter.WriteAsync("21", "KREL", "21", "21", "Katholische Religion");
            await ecfTableWriter.WriteAsync("26", "TECH", "26", "26", "Technik");
            await ecfTableWriter.WriteAsync("27", "PÄD", "27", "27", "Pädagogik");
            await ecfTableWriter.WriteAsync("28", "SPT", "28", "28", "Sport - Theorie");
            await ecfTableWriter.WriteAsync("29", "BWL/RW", "29", "29", "BWL / RW");
            await ecfTableWriter.WriteAsync("30", "BWL/VWL", "30", "30", "BWL / VWL");
            await ecfTableWriter.WriteAsync("31", "VWL", "31", "31", "VWL");
            await ecfTableWriter.WriteAsync("32", "SEM", "32", "32", "Seminar");
            await ecfTableWriter.WriteAsync("33", "GSU", "33", "33", "Gesundheit");
            await ecfTableWriter.WriteAsync("34", "PSY", "34", "34", "Psychologie");
            await ecfTableWriter.WriteAsync("35", "RECH", "35", "35", "Recht");

            return await Task.FromResult(30);
        }

        private async Task<int> ExportTeachers(FbConnection fbConnection, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            string sql = $"select * from \"Lehrer\" where \"Mandant\" = @tenantId and \"Status\" = 1";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            fbCommand.Parameters.Add("@tenantId", _tenantId);

            using var reader = await fbCommand.ExecuteReaderAsync();

            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.LastName,
                    EcfHeaders.FirstName,
                    EcfHeaders.MiddleName,
                    EcfHeaders.Salutation,
                    EcfHeaders.Gender,
                    EcfHeaders.Birthdate,
                    EcfHeaders.Birthname,
                    EcfHeaders.PlaceOfBirth,
                    EcfHeaders.MaritalStatusId,
                    EcfHeaders.AddressLines,
                    EcfHeaders.PostalCode,
                    EcfHeaders.Locality,
                    //EcfHeaders.CountryId,
                    EcfHeaders.HomePhoneNumber,
                    EcfHeaders.OfficePhoneNumber,
                    EcfHeaders.EmailAddress,
                    EcfHeaders.MobileNumber,
                    //EcfHeaders.Nationality1Id,
                    //EcfHeaders.Nationality2Id,
                    EcfHeaders.NativeLanguageId,
                    EcfHeaders.CorrespondenceLanguageId);
                    //EcfHeaders.ReligionId); 
            }

            while (await reader.ReadAsync())
            {
                ecfTableWriter.SetValue(EcfHeaders.Id, reader["ID"]);
                ecfTableWriter.SetValue(EcfHeaders.Code, reader["Kuerzel"]);
                ecfTableWriter.SetValue(EcfHeaders.LastName, reader["Nachname"]);
                ecfTableWriter.SetValue(EcfHeaders.FirstName, reader["Vorname"]);
                ecfTableWriter.SetValue(EcfHeaders.MiddleName, reader["Vorname2"]);
                ecfTableWriter.SetValue(EcfHeaders.Salutation, reader.GetSalutation("Anrede"));
                ecfTableWriter.SetValue(EcfHeaders.Gender, reader.GetGender("Geschlecht"));
                ecfTableWriter.SetValue(EcfHeaders.Birthdate, reader.GetDate("Geburtsdatum"));
                ecfTableWriter.SetValue(EcfHeaders.Birthname, reader["Geburtsname"]);
                ecfTableWriter.SetValue(EcfHeaders.PlaceOfBirth, reader["Geburtsort"]);
                ecfTableWriter.SetValue(EcfHeaders.MaritalStatusId, reader["Ehestand"]);
                ecfTableWriter.SetValue(EcfHeaders.AddressLines, reader["Strasse"]);
                ecfTableWriter.SetValue(EcfHeaders.PostalCode, reader["PLZ"]);
                ecfTableWriter.SetValue(EcfHeaders.Locality, reader["Ort"]);
                //ecfTableWriter.SetValue(EcfHeaders.CountryId, reader["Land"]);
                ecfTableWriter.SetValue(EcfHeaders.HomePhoneNumber, reader["Telefon"]);
                ecfTableWriter.SetValue(EcfHeaders.OfficePhoneNumber, reader["TelefonDienst"]);
                ecfTableWriter.SetValue(EcfHeaders.EmailAddress, reader["Email"]);
                ecfTableWriter.SetValue(EcfHeaders.MobileNumber, reader["Mobil"]);
                //ecfTableWriter.SetValue(EcfHeaders.Nationality1Id, reader["Staatsangeh"]);
                //ecfTableWriter.SetValue(EcfHeaders.Nationality2Id, reader["Staatsangeh2"]);
                ecfTableWriter.SetValue(EcfHeaders.NativeLanguageId, reader["Muttersprache"]);
                ecfTableWriter.SetValue(EcfHeaders.CorrespondenceLanguageId, reader["Verkehrssprache"]);
                //ecfTableWriter.SetValue(EcfHeaders.ReligionId, reader["Konfession"]);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ReadMagellanVersion(FbConnection fbConnection)
        {
            var sql = "select first 1 \"Release\" from \"Version\"";

            using var fbTransaction = fbConnection.BeginTransaction();
            using var fbCommand = new FbCommand(sql, fbConnection, fbTransaction);

            using var reader = await fbCommand.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                var version = reader.GetInt32(reader.GetOrdinal("Release"));

                if (version >= 800)
                    return 8;
                else if (version >= 700)
                    return 7;
                else
                    return 6;
            }
            else
            {
                return 6;
            }
        }    
    }
}
