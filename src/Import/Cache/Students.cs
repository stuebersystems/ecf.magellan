using System;
using System.Collections.Generic;
using System.Linq;

namespace Ecf.Magellan
{

    // Id;SchoolTermId;SchoolClassId;StudentId;Status
    // 5522-2017-1-11;2017-1;2017-1-11;5522;N


    public struct CareerEcfValues
    {
        /// <summary>
        /// Incoming Career Id
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Incoming SchoolTerm Id
        /// </summary>
        public string SchoolTermId { get; set; }

        /// <summary>
        /// Incoming SchoolClass Id
        /// </summary>
        public string SchoolClassId { get; set; }

        /// <summary>
        /// Incoming Student Id
        /// </summary>
        public string StudentId { get; set; }

        /// <summary>
        /// Status of the career e.g. Normal, Wiederholer
        /// </summary>
        public string Status { get; set; }
    }
    
    public struct CareerMagellanValues
    {
        public int StudentTermId { get; set; }
        public int ClassTermId { get; set; }
        public int SchoolClassId { get; set; }
        public int SchoolTermId { get; set; }

        /// <summary>
        /// Status of the career e.g. +, J, N
        /// </summary>
        public string Gewechselt { get; set; }
    }

    public struct StudentSubjectEcfValues
    {
        /// <summary>
        /// Incoming Subject Id for StudentSubjects
        /// </summary>
        public string SubjectId { get; set; }

        /// <summary>
        /// Incoming Teacher Id for StudentSubjects
        /// </summary>
        public string TeacherId { get; set; }

        /// <summary>
        /// Incoming Grade1Value Id for StudentSubjects
        /// </summary>
        public string Grade1ValueId { get; set; }

        /// <summary>
        /// Incoming CourseNo
        /// </summary>
        public string CourseNo { get; set; }

        /// <summary>
        /// Incoming CourseTypeId
        /// </summary>
        public string CourseTypeId { get; set; }

        /// <summary>
        /// Incoming Grade1AchievementTypeId
        /// </summary>
        public string Grade1AchievementTypeId { get; set; }

        /// <summary>
        /// Incoming Passfail
        /// </summary>
        public string Passfail { get; set; }        
    }
    
    public struct StudentSubjectMagellanValues
    {
        public int SubjectId { get; set; }
        public int TeacherId { get; set; }
        public int Grade1ValueId { get; set; }
    }



    public class StudentSubjects
    {
        /// <summary>
        /// Incoming values from Ecf
        /// </summary>
        public StudentSubjectEcfValues EcfValues;

        /// <summary>
        /// Magellan values found in db
        /// </summary>
        public StudentSubjectMagellanValues MagellanValues;

        public StudentSubjects(string subjectId, string teacherId, string grade1ValueId, string courseNo, 
            string courseTypeId, string grade1AchievementTypeId, string passfail)
        {
            EcfValues.SubjectId = subjectId;
            EcfValues.TeacherId = teacherId;
            EcfValues.Grade1ValueId = grade1ValueId;
            EcfValues.CourseNo = courseNo;
            EcfValues.CourseTypeId = courseTypeId;
            EcfValues.Grade1AchievementTypeId = grade1AchievementTypeId;
            EcfValues.Passfail = passfail;
        }
    }

    public class Career
    {
        /// <summary>
        /// Incoming values from Ecf
        /// </summary>
        public CareerEcfValues EcfValues;

        /// <summary>
        /// Magellan Ids found in db
        /// </summary>
        public CareerMagellanValues MagellanValues;
       
        public List<StudentSubjects> StudentSubjects { get; set; }


        public Career(string Id, string SchoolTermId, string SchoolClassId, string StudentId, string Status)
        {
            EcfValues.Id = Id;
            EcfValues.SchoolTermId = SchoolTermId;
            EcfValues.SchoolClassId = SchoolClassId;
            EcfValues.StudentId = StudentId;
            EcfValues.Status = Status;

            MagellanValues.Gewechselt = "+";

            StudentSubjects = new List<StudentSubjects>();
        }

        public Career(string Id, string SchoolTermId, string SchoolClassId, string StudentId)
        {            
            EcfValues.Id = Id;
            EcfValues.SchoolTermId = SchoolTermId;
            EcfValues.SchoolClassId = SchoolClassId;
            EcfValues.StudentId = StudentId;

            StudentSubjects = new List<StudentSubjects>();
        }

    }

    public class Students
    {
        /// <summary>
        /// Incoming Ids from Ecf
        /// </summary>
        public string EcfId { get; set; }

        /// <summary>
        /// Id in MAGELLAN
        /// </summary>
        public int MagellanId { get; set; }

        public List<Career> Career { get; set; }


        public Students(string ecfId)
        {
            EcfId = ecfId;
            Career = new List<Career>();
        }

        public void SetLastGewechseltToN()
        {
            Career c = Career.Last();
            c.MagellanValues.Gewechselt = "N";
        }

        /// <summary>
        /// Checks if all needed MagellanIds 
        /// for a career are set
        /// </summary>
        public bool ValidateForCareer(out string wrongValues)
        {
            wrongValues = String.Empty;
            if (!(MagellanId > 0)) wrongValues = "StudentId";
            
            if (String.IsNullOrEmpty(wrongValues))
            {
                foreach (var career in Career)
                {
                    if (!(career.MagellanValues.SchoolClassId > 0)) wrongValues = String.Join(",", wrongValues, "SchoolClassId");
                    if (!(career.MagellanValues.SchoolTermId > 0)) wrongValues = String.Join(",", wrongValues, "SchoolTermId");
                    if (!(career.MagellanValues.ClassTermId > 0)) wrongValues = String.Join(",", wrongValues, "SchoolClassTermId");
                    if (!(career.MagellanValues.StudentTermId == 0)) wrongValues = String.Join(",", wrongValues, "StudentTermId");
                                       
                    if (!String.IsNullOrEmpty(wrongValues)) break;
                }
            }

            return String.IsNullOrEmpty(wrongValues);
        }

        /// <summary>
        /// Checks if all needed MagellanIds 
        /// for a StudentSubject are set
        /// </summary>
        public bool ValidateForStudentSubjects(out string missingValues)
        {
            missingValues = String.Empty;
            if (!(MagellanId > 0)) missingValues = "StudentId";

            if (String.IsNullOrEmpty(missingValues))
            {
                foreach (var career in Career)
                {
                    if (!(career.MagellanValues.SchoolClassId > 0)) missingValues = String.Join(",", missingValues, "SchoolClassId");
                    if (!(career.MagellanValues.SchoolTermId > 0)) missingValues = String.Join(",", missingValues, "SchoolTermId");
                    if (!(career.MagellanValues.ClassTermId > 0)) missingValues = String.Join(",", missingValues, "SchoolClassTermId");
                    if (!(career.MagellanValues.StudentTermId > 0)) missingValues = String.Join(",", missingValues, "StudentTermId");


                    if (!String.IsNullOrEmpty(missingValues)) break;
                }
            }

            return String.IsNullOrEmpty(missingValues); 
        }
    }

}