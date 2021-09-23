using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class ValueConvert
    {
        public static string Gender(string value)
        {
            var result = String.Empty;
            if (value == "Female") { result = "W"; } else
            if (value == "Male")   { result = "M"; } else
            if (value == "Divers") { result = "D"; }

            return result;
        }

        public static int GradeSystem(string value)
        {
            /*
             Mögliche Werte in MAGELLAN:
                0 = Notenwerte
                1 = Punktwerte
                2 = Beurteilungen
            */

            switch (value)
            {                
                case "PT":
                    return 1;
                
                case "BU":    // Placeholder for "Beurteilungen", has to be refactored in case 
                    return 2;

                case "0":
                default:
                    return 0;                    
            }
        }
        public static string Notification(string value)
        {
            // Mögliche Werte in MAGELLAN: Immer, Nur im Notfall, Nie  

            var result = String.Empty;
            if (value == "Always") { result = "0"; }
            else
            if (value == "UrgentCasesOnly") { result = "1"; }
            else
            if (value == "Never") { result = "2"; }

            return result;
        }

        public static string Passfail(string value)
        {
            /*
             Mögliche Werte in MAGELLAN:
                P = Bestanden
                F = Nicht bestanden
                N = Nicht belegt
             */

            var result = String.Empty;
            if (value == "Passed")  { result = "P"; } else
            if (value == "Failed")  { result = "F"; } else
            if (value == "NotUsed") { result = "N"; }
            
            return result;
        }

        public static string Priority(string value)
        {
            // Mögliche Werte in MAGELLAN: Telefon privat, Telefon beruf, Mobil

            var result = String.Empty;
            if (value == "HomePhoneNumber") { result = "0"; } else
            if (value == "OfficePhoneNumber") { result = "1"; } else
            if (value == "MobileNumber") { result = "2"; }

            return result;
        }

        public static string Salutation(string value)
        {
            /*
             Mögliche Werte in MAGELLAN:
                0 = Frau
                1 = Herr
                2 = Frau Dr.
                3 = Herr Dr.
                4 = Frau Prof.
                5 = Herr Prof.
                6 = Frau Prof. Dr.
                7 = Herr
                Prof. Dr.
                : = Ms.
                ; = Mrs.
                < = Mr.
             */

            var result = String.Empty;
            if (value == "Herr") { result = "1"; } else
            if (value == "Frau") { result = "0"; }            

            return result;
        }

        public static string RelationShip(string value)
        {
            /*
             Mögliche Werte in MAGELLAN:
                0 = Mutter
                1 = Vater
                2 = Eltern
                3 = Erziehungsberechtigte(r)
                4 = Sorgeberechtigte(r)
                5 = Ansprechpartner(in)
                6 = Vormund
                7 = Großmutter
                8 = Großvater
                9 =Pflegeeltern
                11 = Verhältnis1
                12 = Verhältnis2
                13 = Verhältnis3
                14 = Verhältnis4
                15 = Verhältnis5
                16 = Verhältnis6
                17 = Verhältnis7
                18 = Verhältnis8
                19 = Verhältnis9
                20 = Verhältnis10
                (Werte für die Verhältnis 11 … 20 können über „Bezeichnungen anpassen" ersetzt werden)
                21 = Onkel
                22 = Tante
                23 = Bruder
                24 = Schwester
                25 = Erzieher
                26 = Notfall
                27 = Gasteltern
             */


            var result = String.Empty;
            if (value == "Mother") { result = "0"; } else
            if (value == "Father") { result = "1"; }

            return result;
        }



    }
}
