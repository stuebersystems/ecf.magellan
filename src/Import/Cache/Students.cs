using System;
using System.Collections.Generic;
using System.Linq;

namespace Ecf.Magellan
{

    // Id;SchoolTermId;SchoolClassId;StudentId;Status
    // 5522-2017-1-11;2017-1;2017-1-11;5522;N


    public struct EcfIds
    {
        /// <summary>
        /// Incoming Carrer Id
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
    }

    public struct MagellanIds
    {
        public int SchuelerZeitraumId { get; set; }
        public int KlassenZeitraumId { get; set; }
        public int KlassenId { get; set; }
        public int ZeitraumId { get; set; }
        public int FachId { get; set; }
        public int LehrerId { get; set; }
        public int Endnote1Id { get; set; }
    }

    public class Career
    {
        /// <summary>
        /// Incoming Ids from Ecf
        /// </summary>
        public EcfIds EcfIds;

        /// <summary>
        /// Magellan Ids found in db
        /// </summary>
        public MagellanIds MagellanIds;

        /// <summary>
        /// Status of the career e.g. Normal, Wiederholer
        /// </summary>
        public string EcfStatus { get; set; }

        /// <summary>
        /// Status of the career e.g. +, J, N
        /// </summary>
        public string Gewechselt { get; set; }

        public Career(string Id, string SchoolTermId, string SchoolClassId, string StudentId, string Status)
        {
            EcfIds.Id = Id;
            EcfIds.SchoolTermId = SchoolTermId;
            EcfIds.SchoolClassId = SchoolClassId;
            EcfIds.StudentId = StudentId;
            EcfStatus = Status;

            Gewechselt = "+";
        }

        public Career(string Id, string SchoolTermId, string SchoolClassId, string StudentId, 
            string SubjectId, string TeacherId, string Grade1ValueId)
        {
            EcfIds.Id = Id;
            EcfIds.SchoolTermId = SchoolTermId;
            EcfIds.SchoolClassId = SchoolClassId;
            EcfIds.StudentId = StudentId;
            EcfIds.SubjectId = SubjectId;
            EcfIds.TeacherId = TeacherId;
            EcfIds.Grade1ValueId = Grade1ValueId;            
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
            c.Gewechselt = "N";
        }

        /// <summary>
        /// Checks if all needed MagellanIds 
        /// for a career are set
        /// </summary>
        public bool Validate()
        {
            bool result = (MagellanId > 0);

            if (result)
            {
                foreach (var career in Career)
                {
                    result =
                        (career.MagellanIds.KlassenId > 0) &&
                        (career.MagellanIds.ZeitraumId > 0) &&
                        (career.MagellanIds.SchuelerZeitraumId == 0) &&
                        (career.MagellanIds.KlassenZeitraumId > 0);
                    
                    if (!result) break;
                }
            }

            return result;
        }
    }
}
