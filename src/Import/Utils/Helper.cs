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

                    foreach (var studentSubject in career.StudentSubjects)
                    {
                        dbResult = await RecordExists.Subject(fbConnection, tenantId, studentSubject.EcfValues.SubjectId);
                        if (dbResult.Success)
                        {
                            studentSubject.MagellanValues.SubjectId = (int)dbResult.Value;

                            dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Teachers, tenantId, studentSubject.EcfValues.TeacherId);
                            if (dbResult.Success)
                            {
                                studentSubject.MagellanValues.TeacherId = (int)dbResult.Value;
                            }

                            dbResult = await RecordExists.ByCodeAndTenant(fbConnection, MagellanTables.GradeValues, tenantId, studentSubject.EcfValues.Grade1ValueId);
                            if (dbResult.Success)
                            {
                                studentSubject.MagellanValues.Grade1ValueId = (int)dbResult.Value;
                            }
                        }
                    }                    
                }
            }               
        }

        public static void AddStudentsSubjects(EcfTableReader ecfTableReader, List<Students> students, List<SimpleCache> schoolTerms)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");                
                var student = students.Find(s => s.EcfId.Equals(studentId));
                
                var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                var schoolTerm = schoolTerms.Find(t => t.EcfId.Equals(schoolTermId));

                var classTermId = ecfTableReader.GetValue<string>("SchoolClassId");

                if (!String.IsNullOrEmpty(studentId) && !String.IsNullOrEmpty(schoolTermId) && !String.IsNullOrEmpty(classTermId) && (student != null) && (schoolTerm != null))
                {
                    var career = student.Career.Find(c => c.EcfValues.SchoolTermId.Equals(schoolTermId) && c.EcfValues.ClassTermId.Equals(classTermId));

                    if (career != null)
                    {
                        career.StudentSubjects.Add(new StudentSubjects(
                            ecfTableReader.GetValue<string>("SubjectId"),
                            ecfTableReader.GetValue<string>("TeacherId"),
                            ecfTableReader.GetValue<string>("Grade1ValueId"),
                            ecfTableReader.GetValue<string>("CourseNo"),
                            ecfTableReader.GetValue<string>("CourseTypeId"),
                            ecfTableReader.GetValue<string>("Grade1AchievementTypeId"),
                            ecfTableReader.GetValue<string>("Passfail")
                        ));
                    }
                }
            }
        }

/*
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

*/
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

        public static void ProcessStudents(EcfTableReader ecfTableReader, List<Students> students, List<SimpleCache> schoolTerms)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var student = students.Find(s => s.EcfId.Equals(studentId));
                
                var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                var schoolTerm = schoolTerms.Find(t => t.EcfId.Equals(schoolTermId));

                var classTermId = ecfTableReader.GetValue<string>("SchoolClassId");

                if (!String.IsNullOrEmpty(studentId) && !String.IsNullOrEmpty(schoolTermId) && !String.IsNullOrEmpty(classTermId) && (student != null) && (schoolTerm != null))
                {
                    student.Career.Add(new Career(
                        ecfTableReader.GetValue<string>("Id"),
                        ecfTableReader.GetValue<string>("SchoolTermId"),
                        ecfTableReader.GetValue<string>("SchoolClassId"),                        
                        ecfTableReader.GetValue<string>("Status")
                        )
                    );
                }
            }

            // Sort student careers
            students.ForEach(s => s.Career.Sort((x, y) => x.EcfValues.SchoolTermId.CompareTo(y.EcfValues.SchoolTermId)));

            students.ForEach(s => s.SetLastGewechseltToN());

            //students.ForEach(s => s.Career.ForEach(c => Console.WriteLine($"Student: {c.EcfIds.StudentId} - {c.EcfIds.SchoolTermId} - {c.Gewechselt}")));            
        }

        public static void ProcessStudentsSubjects(EcfTableReader ecfTableReader, List<Students> students)
        {
            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var studentId = ecfTableReader.GetValue<string>("StudentId");
                var student = students.Find(s => s.EcfId.Equals(studentId));
                var schoolTermId = ecfTableReader.GetValue<string>("SchoolTermId");
                var classTermId = ecfTableReader.GetValue<string>("SchoolClassId");
                var subjectId = ecfTableReader.GetValue<string>("SubjectId");

                if (!String.IsNullOrEmpty(studentId) && !String.IsNullOrEmpty(schoolTermId) && !String.IsNullOrEmpty(classTermId) && !String.IsNullOrEmpty(subjectId) && (student != null))
                {
                    var careerId = ecfTableReader.GetValue<string>("Id");
                    var career = student.Career.Find(c => c.EcfValues.Id.Equals(careerId));

                    if (career == null)
                    {
                        career = new Career(
                            ecfTableReader.GetValue<string>("Id"),
                            ecfTableReader.GetValue<string>("SchoolTermId"),
                            ecfTableReader.GetValue<string>("SchoolClassId"),
                            ecfTableReader.GetValue<string>("StudentId")
                        );

                        student.Career.Add(career);
                    }

                    StudentSubjects studentSubjects = new StudentSubjects(
                        ecfTableReader.GetValue<string>("SubjectId"),
                        ecfTableReader.GetValue<string>("TeacherId"),
                        ecfTableReader.GetValue<string>("Grade1ValueId"),
                        ecfTableReader.GetValue<string>("CourseNo"),
                        ecfTableReader.GetValue<string>("CourseTypeId"),
                        ecfTableReader.GetValue<string>("Grade1AchievementTypeId"),
                        ecfTableReader.GetValue<string>("Passfail")
                    );

                    career.StudentSubjects.Add(studentSubjects);
                }
            }
        }

        public static async void SetMagellanIds(FbConnection fbConnection, int tenantId, List<Students> students, List<SimpleCache> schoolTerms)
        {
            foreach (var student in students)
            {                
                foreach (var career in student.Career)
                {
                    // Find SchoolTerm
                    var schoolTerm = schoolTerms.Find(t => t.EcfId.Equals(career.EcfValues.SchoolTermId));
                        
                    if (schoolTerm != null)
                    {
                        career.MagellanValues.SchoolTermId = schoolTerm.MagellanId;

                        // Find SchoolClassTerm
                        DbResult dbResult = await RecordExists.SchoolClassTerm(fbConnection, tenantId, career.EcfValues.ClassTermId);
                        if (dbResult.Success)
                        {
                            career.MagellanValues.ClassTermId = (int)dbResult.Value;

                            // Find SchoolClass
                            dbResult = await RecordExists.SchoolClassByTerm(fbConnection, tenantId, career.EcfValues.ClassTermId);
                            if (dbResult.Success)
                            {
                                career.MagellanValues.SchoolClassId = (int)dbResult.Value;

                                // Find SchoolClassAttendance
                                dbResult = await RecordExists.StudentSchoolClassAttendances(fbConnection, tenantId, career.MagellanValues.ClassTermId, student.MagellanId);
                                if (dbResult.Success)
                                {
                                    career.MagellanValues.StudentTermId = (int)dbResult.Value;
                                }
                            }
                        }
                    }
                }                
            }
        }

        public static async void SetMagellanSubjectIds(FbConnection fbConnection, int tenantId, List<Students> students, List<SimpleCache> schoolTerms)
        {
            foreach (var student in students)
            {                
                foreach (var career in student.Career)
                {
                    foreach (var studentSubject in career.StudentSubjects)
                    {
                        DbResult dbResult = await RecordExists.Subject(fbConnection, tenantId, studentSubject.EcfValues.SubjectId);
                        if (dbResult.Success)
                        {
                            studentSubject.MagellanValues.SubjectId = (int)dbResult.Value;

                            dbResult = await RecordExists.ByGuidExtern(fbConnection, MagellanTables.Teachers, tenantId, studentSubject.EcfValues.TeacherId);
                            if (dbResult.Success)
                            {
                                studentSubject.MagellanValues.TeacherId = (int)dbResult.Value;
                            }

                            dbResult = await RecordExists.ByCodeAndTenant(fbConnection, MagellanTables.GradeValues, tenantId, studentSubject.EcfValues.Grade1ValueId);
                            if (dbResult.Success)
                            {
                                studentSubject.MagellanValues.Grade1ValueId = (int)dbResult.Value;
                            }
                        }
                    }                                                                                                            
                }               
            }
        }

        public static void SetParamValue(FbCommand fbCommand, string paramName, FbDbType paramType, object paramValue, object defaultValue = null)
        {
            if (defaultValue == null) defaultValue = String.Empty;

            if ((paramValue == null) || string.IsNullOrEmpty(paramValue.ToString()))
            {
                if (paramValue == null || string.IsNullOrEmpty(defaultValue.ToString()))
                {
                    fbCommand.Parameters.Add(paramName, DBNull.Value);
                }
                else
                {
                    fbCommand.Parameters.Add(paramName, defaultValue);
                }
            }
            else
            {                
               if (((paramType == FbDbType.BigInt) || (paramType == FbDbType.Integer)) && (Convert.ToInt32(paramValue) < 1) )
                    fbCommand.Parameters.Add(paramName, DBNull.Value);
               else
                    fbCommand.Parameters.Add(paramName, paramType).Value = paramValue;
            }
                
        }      
    }
}
