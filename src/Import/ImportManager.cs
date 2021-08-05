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
        private readonly DirectoryInfo _sourceFolder;
        private int _recordCounter = 0;
        private int _tableCounter = 0;
        private string _currentCsv;
        private readonly List<Students> _students;
        private readonly List<SimpleCache> _schoolTerms;

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
            _schoolTerms = new List<SimpleCache>();
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

                    // processes every found import file in given dependency order
                    if (ecfFiles.Length > 0)
                    {
                        // SchoolTerms, only if not given by configuration
                        if (_schoolTermId < 1)
                            { await Execute(EcfTables.SchoolTerms, connection, async (c, r) => await ImportSchoolTerms(c, r)); } else
                            { await AddSchoolTerm(); } 
                         
                        // Catalogs
                        await Execute(EcfTables.AchievementTypes, connection, async (c, r) => await ImportTokenCatalog(c, r));
                        
                        // do not import, use Standard MAGELLAN Catalog: Nationalities -> Staatsangehoerigkeiten

                        await Execute(EcfTables.CourseTypes, connection, async (c, r) => await ImportTokenCatalog(c, r));                          
                        await Execute(EcfTables.ForeignLanguages, connection, async (c, r) => await ImportSubjects(c, r)); // Mit Kategorie als "Fremdsprachen"

                        // do not import, not needed: GradeSystems                       
                        
                        await Execute(EcfTables.GradeValues, connection, async (c, r) => await ImportTokenCatalog(c, r));
                        await Execute(EcfTables.Languages, connection, async (c, r) => await ImportTokenCatalog(c, r)); 
                        
                        // do not import, not needed: StudentSchoolClassAttendanceStatus
                        
                        
                        // Education

                        // do not import, not needed: Rooms
                                                
                        await Execute(EcfTables.Subjects, connection, async (c, r) => await ImportSubjects(c, r));
                        await Execute(EcfTables.Teachers, connection, async (c, r) => await ImportTeachers(c, r));
                        await Execute(EcfTables.SchoolClasses, connection, async (c, r) => await ImportSchoolClasses(c, r));
                        await Execute(EcfTables.Custodians, connection, async (c, r) => await ImportCustodians(c, r));
                        await Execute(EcfTables.Students, connection, async (c, r) => await ImportStudents(c, r));
                        await Execute(EcfTables.StudentForeignLanguages, connection, async (c, r) => await ImportStudentForeignLanguages(c, r));
                        await Execute(EcfTables.StudentCustodians, connection, async (c, r) => await ImportStudentCustodians(c, r));                        
                        await Execute(EcfTables.StudentSchoolClassAttendances, connection, async (c, r) => await ImportStudentSchoolClassAttendances(c, r));
                        await Execute(EcfTables.StudentSubjects, connection, async (c, r) => await ImportStudentSubjects(c, r));                                                                    
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
                Console.WriteLine($"[Error] Importing failed. Only {_tableCounter} table(s) and {_recordCounter} record(s) imported");
                throw;
            }
        }

        private async Task Execute(string csvTableName, FbConnection fbConnection, Func<FbConnection, EcfTableReader, Task<int>> action)
        {
            var sourceFile = Path.ChangeExtension(Path.Combine(_sourceFolder.FullName, csvTableName), ".csv");
            if (!File.Exists(sourceFile)) return;

            // Report status
            Console.WriteLine($"[Importing] [{csvTableName}] Start...");
            

            // Init CSV file stream
            using var csvfileStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read, FileShare.None);

            // Init CSV Reader
            using var csvReader = new CsvReader(csvfileStream, Encoding.UTF8);

            // Init ECF Reader
            var ecfTableReader = new EcfTableReader(csvReader);
            await ecfTableReader.ReadHeadersAsync();

            _currentCsv = csvTableName;

            // Call table specific action
            var ecfRecordCounter = await action(fbConnection, ecfTableReader);

            // Inc counters
            _recordCounter += ecfRecordCounter;
            _tableCounter++;

            // Report status
            Console.WriteLine($"[Importing] [{csvTableName}] {ecfRecordCounter} record(s) imported");
        }

        private async Task AddSchoolTerm()
        {
            _schoolTerms.Add(new SimpleCache("2021-2", _schoolTermId));

            await Task.CompletedTask;
        }


        private async Task<int> ImportCustodians(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values
                var ecfId = ecfTableReader.GetValue<string>("Id");

                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, ecfId);

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
            static async void InsertSchoolClassAndTerm(FbConnection fbConnection, EcfTableReader ecfTableReader, int tenantId, int schoolTermId, string classTermId)
            {
                DbResult SchoolClassResult = await RecordInsert.SchoolClass(fbConnection, ecfTableReader, tenantId);
                if (SchoolClassResult.Success)
                {
                    await RecordInsert.SchoolClassTerm(fbConnection, tenantId, (int)SchoolClassResult.Value, schoolTermId, classTermId, ecfTableReader);
                }
                else
                {
                    Console.WriteLine("[ERROR] [SchoolClass] could not be inserted");
                }
            }

            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                // read needed field-values                                
                SimpleCache schoolTerm;

                if (_schoolTermId > 0)
                {                    
                    schoolTerm = _schoolTerms.Find(t => t.MagellanId.Equals(_schoolTermId));
                } else
                {
                    var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                    schoolTerm = _schoolTerms.Find(t => t.EcfId.Equals(schoolTermId));
                }

                var schoolClassesId = ecfTableReader.GetValue<string>("FederationId");
                var classTermId = ecfTableReader.GetValue<string>("Id");
                
                if (schoolTerm != null)
                {                     
                    DbResult SchoolClassResult = await RecordExists.SchoolClass(fbConnection, _tenantId, schoolClassesId);
                    if (SchoolClassResult.Success)
                    {
                        // UPDATE                            
                        await RecordUpdate.SchoolClass(fbConnection, ecfTableReader, _tenantId, (int)SchoolClassResult.Value);

                        DbResult SchoolClassTermResult = await RecordExists.SchoolClassTerm(fbConnection, _tenantId, classTermId);
                        if (!SchoolClassTermResult.Success)
                        {
                            // INSERT SchoolClassTerm
                            await RecordInsert.SchoolClassTerm(
                                fbConnection, _tenantId, (int)SchoolClassResult.Value, schoolTerm.MagellanId, classTermId,
                                ecfTableReader);
                        }
                    } 
                    else
                    {
                        // INSERT                           
                        InsertSchoolClassAndTerm(fbConnection, ecfTableReader, _tenantId, schoolTerm.MagellanId, classTermId);
                    }

                    recordCounter += 1;
                }
                else
                {
                    Console.WriteLine("[ERROR] [SchoolTerm] FROM [SchoolClasses] could not be found");
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
                var ecfId = ecfTableReader.GetValue<string>("Id");
                var validFrom = ecfTableReader.GetValue<string>("ValidFrom");
                var validTo = ecfTableReader.GetValue<string>("ValidTo");                
                
                DbResult dbResult = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);

                if (dbResult.Success)
                {
                    // UPDATE
                    _schoolTerms.Add(new SimpleCache(ecfId, (int)dbResult.Value));
                    await RecordUpdate.SchoolTerm(fbConnection, ecfTableReader, (int)dbResult.Value);
                } else
                {
                    // INSERT
                    dbResult = await RecordInsert.SchoolTerm(fbConnection, ecfTableReader);
                    if (dbResult.Success)
                    {
                        _schoolTerms.Add(new SimpleCache(ecfId, (int)dbResult.Value));
                    }
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
                var ecfId = ecfTableReader.GetValue<string>("Id");
                int studentId = -1;

                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, ecfId);

                if (dbResult.Success)
                {
                    // UPDATE
                    studentId = (int)dbResult.Value;
                    await RecordUpdate.Student(fbConnection, ecfTableReader, _tenantId, studentId);
                    
                }
                else
                {
                    // INSERT
                    dbResult = await RecordInsert.Student(fbConnection, ecfTableReader, _tenantId);
                    if (dbResult.Success) studentId = (int)dbResult.Value;                                       
                }

                if (studentId > 0)
                {
                    _students.Add(new Students(ecfId, studentId));
                    recordCounter += 1;
                }
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentCustodians(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {                
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var student = _students.Find(s => s.EcfId.Equals(studentId));
                
                var custodianId = ecfTableReader.GetValue<string>("CustodianId");
                
                if (student != null)
                {
                    DbResult custodianResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map("Custodians"), _tenantId, custodianId);

                    if (custodianResult.Success)
                    {
                        DbResult dbResult = await RecordExists.StudentCustodian(fbConnection, _tenantId, student.MagellanId, (int)custodianResult.Value);

                        if (dbResult.Success)
                        {
                            // UPDATE
                            await RecordUpdate.StudentCustodian(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                        }
                        else
                        {
                            // INSERT
                            await RecordInsert.StudentCustodian(fbConnection, ecfTableReader, _tenantId, student.MagellanId, (int)custodianResult.Value);
                        }

                        recordCounter += 1;
                    }
                    else
                    {
                        // MESSAGE
                        Console.WriteLine($"[StudentCustodians] [Sorgeberechtigte ({custodianId})] nicht gefunden");
                    }
                } else
                {
                    // MESSAGE
                    Console.WriteLine($"[StudentCustodians] [Schueler ({studentId})] nicht gefunden");
                }                
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
                var student = _students.Find(s => s.EcfId.Equals(studentId));
                var languageId = ecfTableReader.GetValue<string>("LanguageId");

                if (student != null)
                {
                    DbResult ForeignLanguageResult = await Helper.GetSubject(fbConnection, subjects, languageId);

                    if (ForeignLanguageResult.Success)
                    {
                        // UPDATE
                        var success = await RecordUpdate.StudentForeignLanguage(fbConnection, ecfTableReader, _tenantId, student.MagellanId, (int)ForeignLanguageResult.Value);
                        if (success) recordCounter += 1;
                    } else
                    {
                        // MESSAGE
                        Console.WriteLine($"[StudenForeignLanguages] [Fremdsprache/Fach ({languageId})] nicht gefunden");
                    }                    
                }
                else
                {
                    // MESSAGE
                    Console.WriteLine($"[StudenForeignLanguages] [Schueler ({studentId})] nicht gefunden");
                }                
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentSchoolClassAttendances(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;            

            // Caches records for further processing and processes it
            Helper.ProcessStudents(ecfTableReader, _students, _schoolTerms);

            // Find existing MagellanIds that are needed to create a student career
            Helper.SetMagellanIds(fbConnection, _tenantId, _students, _schoolTerms);

            foreach (var student in _students)
            {
                if (student.MagellanId > 0)
                {
                    foreach (var career in student.Career)
                    {
                        if (career.Validate(false, out string wrongValues))
                        {
                            if (career.MagellanValues.StudentTermId < 1)
                            {
                                DbResult dbResult = await RecordInsert.StudentSchoolClassAttendance(fbConnection, _tenantId, student.MagellanId, career);
                                if (dbResult.Success)
                                {
                                    int StudentTermId = (int)dbResult.Value;

                                    var success = await RecordInsert.SchuelerKlassen(fbConnection, _tenantId, StudentTermId, career.EcfValues.Status);
                                    if (success)
                                    {
                                        if (await RecordUpdate.StudentStatus(fbConnection, _tenantId, student.MagellanId, 3))
                                        {
                                            career.MagellanValues.StudentTermId = StudentTermId;                                            
                                        }
                                    }
                                }
                            }
                            
                            recordCounter += 1;
                        }
                        else
                        {
                            // MESSAGE
                            Console.WriteLine($"[StudentSchoolAttendances] [Laufbahn] des Schülers {career.EcfValues.Id} nicht korrekt.\nFolgende Felder sind fehlerhaft: {wrongValues}");
                        }                        
                    }
                }
                else
                {
                    // MESSAGE
                    Console.WriteLine($"[StudentSchoolAttendances] [Schüler] {student.EcfId} wurde nicht gefunden.");                    
                }
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportStudentSubjects(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            // Enhance existing cashed records with student subjects values
            Helper.AddStudentsSubjects(ecfTableReader, _students, _schoolTerms);

            //Find existing MagellanIds for the new student subjects values
            Helper.AddMagellanSubjectIds(fbConnection, _tenantId, _students);
            
            foreach (var student in _students)
            {
                if (student.MagellanId > 0)
                {
                    foreach (var career in student.Career)
                    {
                        if (career.Validate(true, out string wrongValues))
                        {
                            foreach (var studentSubject in career.StudentSubjects)
                            {
                                if (studentSubject.MagellanValues.SubjectId > 0)
                                {
                                    DbResult dbResult = await RecordExists.StudentSubject(fbConnection, _tenantId, career.MagellanValues.StudentTermId, studentSubject);
                                    if (dbResult.Success)
                                    {
                                        // UPDATE
                                        await RecordUpdate.StudentSubject(fbConnection, _tenantId, (int)dbResult.Value, studentSubject);
                                    }
                                    else
                                    {
                                        // INSERT
                                        await RecordInsert.StudentSubject(fbConnection, _tenantId, student.MagellanId, career, studentSubject);
                                    }
                                    
                                    recordCounter += 1;
                                }
                                else
                                {
                                    // MESSAGE
                                    Console.WriteLine($"[StudentSubjects] [Fach ({studentSubject.EcfValues.SubjectId})] nicht korrekt. ");
                                }
                            }
                        }
                        else
                        {
                            // MESSAGE                    
                            Console.WriteLine($"[StudentSubjects] [Laufbahn ({career.EcfValues.Id})] des Schülers ({student.EcfId}) nicht korrekt. Folgende Felder sind fehlerhaft: {wrongValues}");
                        }
                    }
                }
                else
                {
                    // MESSAGE                    
                    Console.WriteLine($"[StudentSubjects] [Schüler ({student.EcfId})] wurde nicht gefunden.");                    
                }
            }

            return await Task.FromResult(recordCounter);
        }

        private async Task<int> ImportSubjects(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {                
                var ecfId = ecfTableReader.GetValue<string>("Id");
                
                DbResult dbResult = await RecordExists.Subject(fbConnection, _tenantId, ecfId);
                if (dbResult.Success)
                {
                    // UPDATE
                    await RecordUpdate.Subject(fbConnection, ecfTableReader, _tenantId, (int)dbResult.Value);
                }
                else
                {
                    // INSERT

                    object category = null;
                    if (_currentCsv == "ForeignLanguages" ) category = 0;
                    
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
                var ecfId = ecfTableReader.GetValue<string>("Id");                
                
                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MappingTables.Map(_currentCsv), _tenantId, ecfId);
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
                var ecfId = ecfTableReader.GetValue<string>("Id");
                DbResult dbResult;

                if (_currentCsv == EcfTables.GradeValues)
                    dbResult = await RecordExists.ByCodeAndTenant(fbConnection, MappingTables.Map(_currentCsv), _tenantId, ecfId);
                else
                    dbResult = await RecordExists.TokenCatalog(fbConnection, MappingTables.Map(_currentCsv), ecfId);

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
                            await RecordUpdate.TokenCatalog(fbConnection, ecfTableReader, MappingTables.Map(_currentCsv));
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
