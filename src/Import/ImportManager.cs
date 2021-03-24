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
    public class ImportManager : CustomManager
    {
        private const string DefaultSearchPattern = "*.csv";
        private readonly int _schoolTermId;
        private readonly int _tenantId;
        private DirectoryInfo _sourceFolder;
        private int _recordCounter = 0;
        private int _tableCounter = 0;
        private string _currentCsv;
        private List<Students> _students;

        public ImportManager(
            Configuration config,
            CancellationToken cancellationToken, 
            EventWaitHandle cancellationEvent)
            : base(config, cancellationToken, cancellationEvent)
        {
            _tenantId = _config.EcfImport.TenantId;
            _schoolTermId = _config.EcfImport.SchoolTermId;
            _sourceFolder = new DirectoryInfo(_config.EcfImport.SourceFolderName);
            _students = new List<Students>();
        }

        public override async Task Execute()
        {
            using var connection = new FbConnection(_config.EcfImport?.DatabaseConnection);
            try
            {
                connection.Open();
                try
                {
                    _tableCounter = 0;
                    _recordCounter = 0;

                    // read all import-files in source folder 
                    var ecfFiles = _sourceFolder.GetFiles(DefaultSearchPattern);

                    // if found import every found file in given dependency order
                    if (ecfFiles.Length > 0)
                    {
                        // Report status

                        
                         
                        // SchoolTerms
                        await Execute(EcfTables.SchoolTerms, connection, async (c, r) => await ImportSchoolTerms(c, r));

                        // Catalogs
                        await Execute(EcfTables.AchievementTypes, connection, async (c, r) => await ImportTokenCatalog(c, r));
                        
                        // do not import, use Standard MAGELLAN Catalog: Nationalities

                        await Execute(EcfTables.CourseTypes, connection, async (c, r) => await ImportTokenCatalog(c, r));  
                        
                        await Execute(EcfTables.ForeignLanguages, connection, async (c, r) => await ImportSubjects(c, r)); // Mit Kategorie als "Fremdsprachen"

                        // do not import, not needed: GradeSystems
                        
                        
                        await Execute(EcfTables.GradeValues, connection, async (c, r) => await ImportTokenCatalog(c, r));

                        await Execute(EcfTables.NativeLanguages, connection, async (c, r) => await ImportTokenCatalog(c, r)); 
                        
                        // do not import, not needed: StudentSchoolClassAttendanceStatus
                        
                        
                        // Education

                        // do not import, not needed: Rooms
                        
                        // do not import, need fixes from Frank
                        await Execute(EcfTables.Subjects, connection, async (c, r) => await ImportSubjects(c, r));                                // Ecf-Datei hat Fehler!

                        await Execute(EcfTables.Teachers, connection, async (c, r) => await ImportTeachers(c, r));
                        await Execute(EcfTables.SchoolClasses, connection, async (c, r) => await ImportSchoolClasses(c, r)); 
                        await Execute(EcfTables.Custodians, connection, async (c, r) => await ImportCustodians(c, r));
                        await Execute(EcfTables.Students, connection, async (c, r) => await ImportStudents(c, r));
                        await Execute(EcfTables.StudentForeignLanguages, connection, async (c, r) => await ImportStudentForeignLanguages(c, r));
                        await Execute(EcfTables.StudentCustodians, connection, async (c, r) => await ImportStudentCustodians(c, r));
                        
                        /*
                        await Execute(EcfTables.StudentSchoolClassAttendances, connection, async (c, r) => await ImportStudentSchoolClassAttendances(c, r));
                        await Execute(EcfTables.StudentSubjects, connection, async (c, r) => await ImportStudentSubjects(c, r));
                        */


                    }
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

        private async Task Execute(string csvTableName, FbConnection fbConnection, Func<FbConnection, EcfTableReader, Task<int>> action)
        {
            var sourceFile = Path.ChangeExtension(Path.Combine(_sourceFolder.FullName, csvTableName), ".csv");
            if (!File.Exists(sourceFile)) return;

            // Report status
            Console.WriteLine($"[Extracting] [{csvTableName}] Start...");
            

            // Init CSV file stream
            using var csvfileStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read, FileShare.None);

            // Init CSV Reader
            using var csvReader = new CsvReader(csvfileStream, Encoding.UTF8);

            // Read headline
            var strHeaders = await csvReader.ReadLineAsync();
            CsvHeaders csvHeader = new CsvHeaders(strHeaders);

            // Init ECF Reader
            var ecfTableReader = new EcfTableReader(csvReader, csvHeader);

            _currentCsv = csvTableName;

            // Call table specific action
            var ecfRecordCounter = await action(fbConnection, ecfTableReader);

            // Inc counters
            _recordCounter += ecfRecordCounter;
            _tableCounter++;

            // Report status
            Console.WriteLine($"[Extracting] [{csvTableName}] {ecfRecordCounter} record(s) extracted");
        }

        private async Task<int> ImportCustodians(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;


            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var Id = ecfTableReader.GetValue<string>("Id");

                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, Id);

                if (dbResult.Success)
                {
                    // UPDATE
                    await RecordUpdate.Custodian(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                }
                else
                {
                    // INSERT
                    await RecordInsert.Custodian(fbConnection, ecfTableReader, _tenantId);
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportSchoolClasses(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                var schoolClassesId = ecfTableReader.GetValue<string>("Id");

                if (Helper.GetSchoolTermParts(schoolTermId, out string validFrom, out string validTo))
                {
                    DbResult schoolTermResult = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);

                    if (schoolTermResult.Success)
                    {
                        DbResult SchoolClassTermResult = await RecordExists.SchoolClassTerm(fbConnection, (int)schoolTermResult.Value, schoolClassesId);
                        if (SchoolClassTermResult.Success)
                        {
                            // UPDATE
                            await RecordUpdate.SchoolClass(fbConnection, ecfTableReader, _tenantId, schoolClassesId);
                        } else
                        {
                            // INSERT

                            DbResult SchoolClassResult = await RecordInsert.SchoolClass(fbConnection, ecfTableReader, _tenantId);

                            if (SchoolClassResult.Success)
                            {
                                await RecordInsert.SchoolClassTerm(fbConnection, _tenantId, (int)SchoolClassResult.Value, (int)schoolTermResult.Value);
                            }
                            else
                            {
                                Console.WriteLine("[ERROR] [SchoolClass] could not be inserted");
                            }
                        }
                    } else
                    {
                        Console.WriteLine("[ERROR] [SchoolTerm] FROM [SchoolClasses] could not be found");
                    }
                    
                    recordCounter += 1;
                }
                else
                {
                    Console.WriteLine("[ERROR] reading [SchoolTerms] FROM [SchoolClasses]");
                }

            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportSchoolTerms(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var validFrom = ecfTableReader.GetValue<string>("ValidFrom");
                var validTo = ecfTableReader.GetValue<string>("ValidTo");                

                // returns - 1 if not exists, otherwise id of record in database
                DbResult dbResult = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);

                if (dbResult.Success)
                {
                    // UPDATE

                    // DEBUG
                    // Console.WriteLine("[UPDATE] [SchoolTerms] TO [Zeitraeume]");

                    await RecordUpdate.SchoolTerm(fbConnection, ecfTableReader, (int)dbResult.Value);
                } else
                {
                    // INSERT

                    // DEBUG
                    // Console.WriteLine("[INSERT] [SchoolTerms] TO [Zeitraeume]");

                    await RecordInsert.SchoolTerm(fbConnection, ecfTableReader);
                }
                
                recordCounter += 1;
            }            

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudents(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var Id = ecfTableReader.GetValue<string>("Id");

                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, Id);

                if (dbResult.Success)
                {
                    // UPDATE
                    await RecordUpdate.Student(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                }
                else
                {
                    // INSERT
                    await RecordInsert.Student(fbConnection, ecfTableReader, _tenantId);
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentCustodians(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var custodianId = ecfTableReader.GetValue<string>("CustodianId");

                DbResult studentResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map("Students"), _tenantId, studentId);
                DbResult custodianResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map("Custodians"), _tenantId, custodianId);

                if (studentResult.Success && custodianResult.Success)
                {
                    DbResult dbResult = await RecordExists.StudentCustodian(fbConnection, _tenantId, (int)studentResult.Value, (int)custodianResult.Value);

                    if (dbResult.Success)
                    {
                        // UPDATE
                        await RecordUpdate.StudentCustodian(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                    }
                    else
                    {
                        // INSERT
                        await RecordInsert.StudentCustodian(fbConnection, ecfTableReader, _tenantId, (int)studentResult.Value, (int)custodianResult.Value);
                    }
                }
                else
                {
                    // MESSAGE
                    Console.WriteLine($"[StudentCustodians] [Schueler/Sorgeberechtigte] nicht gefunden");
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentForeignLanguages(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;
            var subjects = new Dictionary<string, int>();

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var languageCode = ecfTableReader.GetValue<string>("LanguageId");

                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, studentId);

                if (dbResult.Success)
                {
                    DbResult ForeignLanguageResult = await Helper.GetSubject(fbConnection, subjects, languageCode);

                    if (ForeignLanguageResult.Success)
                    {
                        // UPDATE
                        await RecordUpdate.StudentForeignLanguage(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value, (int)ForeignLanguageResult.Value);
                    } else
                    {
                        // MESSAGE
                        Console.WriteLine($"[StudenForeignLanguages] [ForeignLanguage] nicht gefunden");
                    }                    
                }
                else
                {
                    // MESSAGE
                    Console.WriteLine($"[StudenForeignLanguages] [Schueler] nicht gefunden");
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentSchoolClassAttendances(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            // Caches records for further processing and processes it
            Helper.ProcessStudents(ecfTableReader, _students);

            // Find existing MagellanIds that are needed to create a student career
            Helper.SetMagellanIds(fbConnection, _tenantId, _students);

            foreach (var student in _students)
            {
                if (student.Validate())
                {
                    foreach (var career in student.Career)
                    {
                        DbResult dbResult = await RecordInsert.StudentSchoolClassAttendance(fbConnection, ecfTableReader, _tenantId, student.MagellanId, career.MagellanIds, career.Gewechselt);
                        if (dbResult.Success)
                        {
                            var success = await RecordInsert.SchuelerKlassen(fbConnection, _tenantId, (int)dbResult.Value, career.EcfStatus);
                            if (success)
                            {
                                if (await RecordUpdate.StudentStatus(fbConnection, _tenantId, student.MagellanId, 3))
                                {
                                    recordCounter += 1;
                                }
                            }
                        }
                    }
                }
                else
                {
                    // MESSAGE
                    if (student.MagellanId == 0)
                        Console.WriteLine($"[StudentSchoolAttendances] [Schüler] {student.EcfId.ToString()} wurde nicht gefunden.");
                    else
                        Console.WriteLine($"[StudentSchoolAttendances] [Laufbahn] des Schülers {student.EcfId.ToString()} nicht korrekt.");
                }
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentSubjects(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;
            
            if (_students.Count == 0)
            {
                // Caches records for further processing and processes it; If not cashed before
                Helper.ProcessStudentsSubjects(ecfTableReader, _students);

                // Find existing MagellanIds that are needed to create StudentSubjects
                Helper.SetMagellanSubjectIds(fbConnection, _tenantId, _students);                
            } else
            {
                // Enhance existing cashed records with student subjects values
                Helper.AddStudentsSubjects(ecfTableReader, _students);

                //Find existing MagellanIds for the new student subjects values
                Helper.AddMagellanSubjectIds(fbConnection, _tenantId, _students);
            }

            foreach (var student in _students)
            {
                if (student.Validate())
                {
                    foreach (var career in student.Career)
                    {
                        DbResult dbResult = await RecordExists.StudentSubject(fbConnection, ecfTableReader, _tenantId, career.MagellanIds);
                        if (dbResult.Success)
                        {
                            // UPDATE
                            await RecordUpdate.StudentSubject(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value, career.MagellanIds);
                        } else
                        {
                            // INSERT
                            await RecordInsert.StudentSubject(fbConnection, ecfTableReader, _tenantId, student.MagellanId, career.MagellanIds);
                        }

                        recordCounter += 1;
                    }
                }
                else
                {
                    // MESSAGE
                    if (student.MagellanId == 0)
                        Console.WriteLine($"[StudentSubjects] [Schüler] {student.EcfId.ToString()} wurde nicht gefunden.");
                    else
                        Console.WriteLine($"[StudentSubjects] [Laufbahn] des Schülers {student.EcfId.ToString()} nicht korrekt.");
                }
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportSubjects(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var code = ecfTableReader.GetValue<string>("Code");
                DbResult dbResult = await RecordExists.Subject(fbConnection, _tenantId, code);

                if (dbResult.Success)
                {
                    // UPDATE
                    await RecordUpdate.Subject(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                }
                else
                {
                    // INSERT
                    
                    object category = null;
                    if (_currentCsv == "ForeignLanguages") category = 0;

                    await RecordInsert.Subject(fbConnection, ecfTableReader, _tenantId, category);
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportTeachers(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {                
                var id = ecfTableReader.GetValue<string>("Id");                
                
                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, id);
                if (dbResult.Success)
                {
                    // UPDATE
                    await RecordUpdate.Teacher(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                }
                else
                {
                    // INSERT
                    await RecordInsert.Teacher(fbConnection, ecfTableReader, _tenantId);
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportTokenCatalog(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var code = ecfTableReader.GetValue<string>("Code");
                DbResult dbResult;

                if (_currentCsv == EcfTables.GradeValues)
                    dbResult = await RecordExists.ByCodeAndTenant(fbConnection, MappingTables.Map(_currentCsv), _tenantId, code);
                else
                    dbResult = await RecordExists.TokenCatalog(fbConnection, MappingTables.Map(_currentCsv), code);

                if (dbResult.Success)
                {
                    // UPDATE                   

                    switch (_currentCsv)
                    {
                        case EcfTables.AchievementTypes:
                            await RecordUpdate.AchievementType(fbConnection, ecfTableReader);
                            break;

                        case EcfTables.GradeValues:
                            await RecordUpdate.GradeValue(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                            break;

                        default:
                            // CourseTypes, NativeLanguages
                            await RecordUpdate.TokenCatalog(fbConnection, ecfTableReader, code);
                            break;
                    }               
                }
                else
                {
                    // INSERT

                    switch (_currentCsv)
                    {
                        case EcfTables.AchievementTypes:
                            await RecordInsert.AchievementType(fbConnection, ecfTableReader);
                            break;

                        case EcfTables.GradeValues:
                            await RecordInsert.GradeValue(fbConnection, ecfTableReader, _tenantId);
                            break;

                        default:
                            await RecordInsert.TokenCatalog(fbConnection, ecfTableReader, MappingTables.Map(_currentCsv));
                            break;
                    }
                }

                recordCounter += 1;
            }

            return await Task.FromResult(recordCounter);
        }
    }
}
