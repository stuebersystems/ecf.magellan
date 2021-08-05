using Enbrea.Ecf;
using System;
using System.Collections.Generic;
using System.Text;

namespace Ecf.Magellan
{
    public static class MappingTables
    {
        private static Dictionary<string, string> _mappings;

        static MappingTables()
        {
            _mappings = new Dictionary<string, string>()
            {
                {EcfTables.AchievementTypes, MagellanTables.AchievementTypes},
                {EcfTables.CourseTypes, MagellanTables.CourseTypes},
                {EcfTables.Custodians, MagellanTables.Custodians},
                {EcfTables.ForeignLanguages, MagellanTables.Subjects},
                {EcfTables.GradeValues, MagellanTables.GradeValues},
                {EcfTables.Languages, MagellanTables.NativeLanguages},
                {EcfTables.SchoolClasses, MagellanTables.SchoolClasses},
                {EcfTables.SchoolTerms, MagellanTables.SchoolTerms},
                {EcfTables.Students, MagellanTables.Students},
                {EcfTables.StudentSchoolClassAttendances, MagellanTables.StudentSchoolClassAttendances},
                {EcfTables.StudentCustodians, MagellanTables.StudentCustodians},
                {EcfTables.StudentForeignLanguages, MagellanTables.Students},
                {EcfTables.Subjects, MagellanTables.Subjects},
                {EcfTables.Teachers, MagellanTables.Teachers}
            };            
        }

        public static string Map(string key)
        {
            if (_mappings.TryGetValue(key, out var mappingValue))
            {
                return mappingValue;
            }
            else
            {
                return default;
            }
        }
    }
}