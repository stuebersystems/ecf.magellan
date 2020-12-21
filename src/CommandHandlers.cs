#region ENBREA - Copyright (C) 2020 STÜBER SYSTEMS GmbH
/*    
 *    ENBREA
 *    
 *    Copyright (C) 2020 STÜBER SYSTEMS GmbH
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

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class CommandHandlers
    {
        public static async Task Export(FileInfo configFile)
        {
            await Execute(async (cancellationToken, cancellationEvent) =>
            {
                var config = await ConfigurationManager.LoadFromFile(configFile.FullName, cancellationToken);

                var exportManager = new ExportManager(
                    config,
                    cancellationToken, cancellationEvent);
                try
                {
                    await exportManager.Execute();
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine($"[Error] Export failed");
                    Console.WriteLine($"[Error] {ex.Message}");
                    Environment.ExitCode = 1;
                }
            });
        }

        public static async Task Import(FileInfo configFile)
        {
            await Execute(async (cancellationToken, cancellationEvent) =>
            {
                var config = await ConfigurationManager.LoadFromFile(configFile.FullName, cancellationToken);

                var exportManager = new ImportManager(
                    config,
                    cancellationToken, cancellationEvent);
                try
                {
                    await exportManager.Execute();
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine($"[Error] Import failed");
                    Console.WriteLine($"[Error] {ex.Message}");
                    Environment.ExitCode = 1;
                }
            });
        }

        public static async Task InitExport(FileInfo configFile)
        {
            await Execute(async (cancellationToken, cancellationEvent) =>
            {
                try
                {
                    await ConfigurationManager.InitConfig(
                        configFile.FullName,
                        GetTemplateFileName(TemplateType.Export),
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine($"[Error] Template creation failed");
                    Console.WriteLine($"[Error] {ex.Message}");
                    Environment.ExitCode = 1;
                }
            });
        }

        public static async Task InitImport(FileInfo configFile)
        {
            await Execute(async (cancellationToken, cancellationEvent) =>
            {
                try
                {
                    await ConfigurationManager.InitConfig(
                        configFile.FullName,
                        GetTemplateFileName(TemplateType.Import),
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine($"[Error] Template creation failed");
                    Console.WriteLine($"[Error] {ex.Message}");
                    Environment.ExitCode = 1;
                }
            });
        }

        private static async Task Execute(Func<CancellationToken, EventWaitHandle, Task> action)
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            using var cancellationEvent = new EventWaitHandle(false, EventResetMode.AutoReset);

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cancellationTokenSource.Cancel();
                cancellationEvent.Set();
            };

            var stopwatch = new Stopwatch();

            stopwatch.Start();

            try
            {
                await action(cancellationTokenSource.Token, cancellationEvent);
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine($"[Error] Console failed");
                Console.WriteLine($"[Error] {ex.Message}");
                Environment.ExitCode = 1;
            }

            stopwatch.Stop();

            Console.WriteLine();
            Console.WriteLine($"Time elapsed: {stopwatch.Elapsed}.");
        }

        private static string GetTemplateFileName(TemplateType type)
        {
            // Get own assembly info
            Assembly assembly = Assembly.GetExecutingAssembly();

            // Get filename for json template
            return type switch
            {
                TemplateType.Export => Path.Combine(Path.GetDirectoryName(assembly.Location), "Templates", "Template.export.json"),
                TemplateType.Import => Path.Combine(Path.GetDirectoryName(assembly.Location), "Templates", "Template.import.json"),
                _ => throw new NotSupportedException("Template type not supported.")
            };
        }
    }
}