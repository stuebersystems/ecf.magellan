using Enbrea.Ecf;
using FirebirdSql.Data.FirebirdClient;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class Helper
    {
        public static async void AddMagellanSubjectIds(FbConnection fbConnection, int tenantId, List<Students> students)
        {
            DbResult dbResult;

            foreach (var student in students)
            {                
                foreach (var career in student.Career)
                {
                    dbResult = await RecordExists.Subject(fbConnection, tenantId, career.EcfIds.SubjectId);
                    if (dbResult.Success)
                    {
                        career.MagellanIds.FachId = (int)dbResult.Value;

                        dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Teachers, tenantId, career.EcfIds.TeacherId);
                        if (dbResult.Success)
                        {
                            career.MagellanIds.LehrerId = (int)dbResult.Value;
                        }

                        dbResult = await RecordExists.ByCodeAndTenant(fbConnection, MagellanTables.GradeValues, tenantId, career.EcfIds.Grade1ValueId);
                        if (dbResult.Success)
                        {
                            career.MagellanIds.Endnote1Id = (int)dbResult.Value;
                        }
                    }
                }
            }               
        }

        public static void AddStudentsSubjects(EcfTableReader ecfTableReader, List<Students> students)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");                
                var student = students.Find(s => s.EcfId.Equals(studentId));

                if (student == null)
                {
                    student = new Students(studentId);
                    students.Add(student);
                }

                var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                var schoolClassId = ecfTableReader.GetValue<string>("SchoolClassId");                

                var career = student.Career.Find(c => c.EcfIds.SchoolTermId.Equals(schoolTermId) && c.EcfIds.SchoolClassId.Equals(schoolClassId));

                if (career == null)
                {
                    student.Career.Add(new Career(
                        ecfTableReader.GetValue<string>("Id"),
                        ecfTableReader.GetValue<string>("SchoolTermId"),
                        ecfTableReader.GetValue<string>("SchoolClassId"),
                        ecfTableReader.GetValue<string>("StudentId"),
                        ecfTableReader.GetValue<string>("SubjectId"),
                        ecfTableReader.GetValue<string>("TeacherId"),
                        ecfTableReader.GetValue<string>("Grade1ValueId")
                        )
                    );
                } else
                {
                    career.EcfIds.SubjectId = ecfTableReader.GetValue<string>("SubjectId");
                    career.EcfIds.TeacherId = ecfTableReader.GetValue<string>("TeacherId");
                    career.EcfIds.Grade1ValueId = ecfTableReader.GetValue<string>("Grade1ValueId");
                }                
            }
        }

        public static bool GetSchoolTermParts(string schoolTermId, out string validFrom, out string validTo)
        {
            string[] schoolTermParts = schoolTermId.Split('-');
            validFrom = null;
            validTo = null;

            if (schoolTermParts.Length == 2)
            {
                int year = -1;
                int section = -1;

                if (!String.IsNullOrEmpty(schoolTermParts[0]))
                {
                    year = int.Parse(schoolTermParts[0]);
                }

                if (!String.IsNullOrEmpty(schoolTermParts[1]))
                {
                    section = int.Parse(schoolTermParts[1]);
                }

                switch (section)
                {
                    case 1:
                        validFrom = $"01.08.{year}";
                        validTo = $"31.01.{year + 1}";
                        break;
                    case 2:
                        validFrom =$"01.02.{year + 1}";
                        validTo = $"31.07.{year + 1}";
                        break;
                }

                return true;
            }

            return false;
        }

        public static async Task<DbResult> GetSubject(FbConnection fbConnection, Dictionary<string, int> subjects, string code)
        {
            if (subjects.TryGetValue(code, out int id))
            {
                return new DbResult(true, id);
            } else
            {
                DbResult dbResult = await RecordExists.ByCode(fbConnection, MappingTables.Map("ForeignLanguages"), code);
                return dbResult;
            }
        }

        public static void ProcessStudents(EcfTableReader ecfTableReader, List<Students> students)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var student = students.Find(s => s.EcfId.Equals(studentId));

                if (student == null)
                {
                    student = new Students(studentId);
                    students.Add(student);
                }

                student.Career.Add(new Career(
                    ecfTableReader.GetValue<string>("Id"),
                    ecfTableReader.GetValue<string>("SchoolTermId"),
                    ecfTableReader.GetValue<string>("SchoolClassId"),
                    ecfTableReader.GetValue<string>("StudentId"),
                    ecfTableReader.GetValue<string>("Status")
                    )
                );
            }

            // Sort student careers
            students.ForEach(s => s.Career.Sort((x, y) => x.EcfIds.SchoolTermId.CompareTo(y.EcfIds.SchoolTermId)));

            students.ForEach(s => s.SetLastGewechseltToN());

            //students.ForEach(s => s.Career.ForEach(c => Console.WriteLine($"Student: {c.EcfIds.StudentId} - {c.EcfIds.SchoolTermId} - {c.Gewechselt}")));            
        }

        public static void ProcessStudentsSubjects(EcfTableReader ecfTableReader, List<Students> students)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var student = students.Find(s => s.EcfId.Equals(studentId));

                if (student == null)
                {
                    student = new Students(studentId);
                    students.Add(student);
                }

                student.Career.Add(new Career(
                    ecfTableReader.GetValue<string>("Id"),
                    ecfTableReader.GetValue<string>("SchoolTermId"),
                    ecfTableReader.GetValue<string>("SchoolClassId"),
                    ecfTableReader.GetValue<string>("StudentId"),
                    ecfTableReader.GetValue<string>("SubjectId"),
                    ecfTableReader.GetValue<string>("TeacherId"),
                    ecfTableReader.GetValue<string>("Grade1ValueId")                    
                    )
                );
            }
        }

        public static async void SetMagellanIds(FbConnection fbConnection, int tenantId, List<Students> students)
        {
            foreach (var student in students)
            {
                // Find Student
                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Students, tenantId, student.EcfId);
                if (dbResult.Success)
                {
                    student.MagellanId = (int)dbResult.Value;

                    foreach (var career in student.Career)
                    {
                        // Find SchoolTerm
                        GetSchoolTermParts(career.EcfIds.SchoolTermId, out string validFrom, out string validTo);
                        dbResult = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);
                        if (dbResult.Success)
                        {
                            career.MagellanIds.ZeitraumId = (int)dbResult.Value;

                            // Find SchoolClass
                            dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.SchoolClasses, tenantId, career.EcfIds.SchoolClassId);
                            if (dbResult.Success)
                            {
                                career.MagellanIds.KlassenId = (int)dbResult.Value;

                                // Find SchoolClassTerm

                                dbResult = await RecordExists.SchoolClassTerm(fbConnection, career.MagellanIds.ZeitraumId, career.EcfIds.SchoolClassId);
                                if (dbResult.Success)
                                {
                                    career.MagellanIds.KlassenZeitraumId = (int)dbResult.Value;

                                    // Find SchoolClassAttendance
                                    dbResult = await RecordExists.StudentSchoolClassAttendances(fbConnection, tenantId, career.MagellanIds.KlassenZeitraumId, student.MagellanId);
                                    if (dbResult.Success)
                                    {
                                        career.MagellanIds.SchuelerZeitraumId = (int)dbResult.Value;
                                    }
                                }
                            }
                        }
                    }

                }
            }
        }

        public static async void SetMagellanSubjectIds(FbConnection fbConnection, int tenantId, List<Students> students)
        {
            foreach (var student in students)
            {
                // Find Student
                DbResult dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Students, tenantId, student.EcfId);
                if (dbResult.Success)
                {
                    student.MagellanId = (int)dbResult.Value;

                    foreach (var career in student.Career)
                    {
                        // Find SchoolTerm
                        GetSchoolTermParts(career.EcfIds.SchoolTermId, out string validFrom, out string validTo);
                        dbResult = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);
                        if (dbResult.Success)
                        {
                            career.MagellanIds.ZeitraumId = (int)dbResult.Value;

                            // Find SchoolClass
                            dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.SchoolClasses, tenantId, career.EcfIds.SchoolClassId);
                            if (dbResult.Success)
                            {
                                career.MagellanIds.KlassenId = (int)dbResult.Value;

                                // Find SchoolClassTerm
                                dbResult = await RecordExists.SchoolClassTerm(fbConnection, career.MagellanIds.ZeitraumId, career.EcfIds.SchoolClassId);
                                if (dbResult.Success)
                                {
                                    career.MagellanIds.KlassenZeitraumId = (int)dbResult.Value;

                                    // Find SchoolClassAttendance
                                    dbResult = await RecordExists.StudentSchoolClassAttendances(fbConnection, tenantId, career.MagellanIds.KlassenZeitraumId, student.MagellanId);
                                    if (dbResult.Success)
                                    {
                                        career.MagellanIds.SchuelerZeitraumId = (int)dbResult.Value;

                                        dbResult = await RecordExists.Subject(fbConnection, tenantId, career.EcfIds.SubjectId);
                                        if (dbResult.Success)
                                        {
                                            career.MagellanIds.FachId = (int)dbResult.Value;

                                            dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Teachers, tenantId, career.EcfIds.TeacherId);
                                            if (dbResult.Success)
                                            {
                                                career.MagellanIds.LehrerId = (int)dbResult.Value;
                                            }

                                            dbResult = await RecordExists.ByCode(fbConnection, MagellanTables.GradeValues, career.EcfIds.Grade1ValueId);
                                            if (dbResult.Success)
                                            {
                                                career.MagellanIds.Endnote1Id = (int)dbResult.Value;
                                            }
                                        }                                        
                                    }
                                }
                            }
                        }
                    }

                }
            }
        }


        public static void SetParamValue(FbCommand fbCommand, string ParamName, FbDbType ParamType, object ParamValue, object DefaultValue = null)
        {
            if (DefaultValue == null) DefaultValue = String.Empty;

            if ((ParamValue == null) || string.IsNullOrEmpty(ParamValue.ToString()))
            {
                if (ParamValue == null || string.IsNullOrEmpty(DefaultValue.ToString()))
                {
                    fbCommand.Parameters.Add(ParamName, DBNull.Value);
                }
                else
                {
                    fbCommand.Parameters.Add(ParamName, DefaultValue);
                }
            }
            else
                fbCommand.Parameters.Add(ParamName, ParamType).Value = ParamValue;
        }      
    }
}
