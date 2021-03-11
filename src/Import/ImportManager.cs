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

        public ImportManager(
            Configuration config,
            CancellationToken cancellationToken, 
            EventWaitHandle cancellationEvent)
            : base(config, cancellationToken, cancellationEvent)
        {
            _tenantId = _config.EcfImport.TenantId;
            _schoolTermId = _config.EcfImport.SchoolTermId;
            _sourceFolder = new DirectoryInfo(_config.EcfImport.SourceFolderName);
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

                        // Education

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
            var sourceFile = Path.Combine(_sourceFolder.FullName, csvTableName);
            if (!File.Exists(sourceFile)) return;

            // Report status
            Console.WriteLine($"[Extracting] [{csvTableName}] Start...");
            

            // Init CSV file stream
            using var csvfileStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read, FileShare.None);

            // Init CSV Reader
            using var csvReader = new CsvReader(csvfileStream, Encoding.UTF8);

            // Init ECF Reader
            var ecfTablefReader = new EcfTableReader(csvReader);

            // Call table specific action
            var ecfRecordCounter = await action(fbConnection, ecfTablefReader);

            // Inc counters
            _recordCounter += ecfRecordCounter;
            _tableCounter++;

            // Report status
            Console.WriteLine($"[Extracting] [{csvTableName}] {ecfRecordCounter} record(s) extracted");
        }

        private async Task<int> ImportSchoolTerms(FbConnection fbConnection, EcfTableReader ecfTableReader)
        {
            var recordCounter = 0;

            while (ecfTableReader.ReadAsync().Result > 0)
            {
                var validFrom = ecfTableReader.GetValue<string>("ValidFrom");
                var validTo = ecfTableReader.GetValue<string>("ValidTo");

                var id = await RecordExists.SchoolTerm(fbConnection, validFrom, validTo);


                if (id == -1) {
                    // INSERT

                    
                } 
                else
                {
                    // UPDATE

                }



                recordCounter += 1;
            }            

            return await Task.FromResult(recordCounter);
        }

    }
}
