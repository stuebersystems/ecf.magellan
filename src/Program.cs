﻿#region ENBREA - Copyright (C) 2020 STÜBER SYSTEMS GmbH
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
using System.CommandLine;
using System.Text;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            // Console window title
            Console.Title = AssemblyInfo.GetTitle();

            // Display infos about this app
            Console.WriteLine();
            Console.WriteLine(AssemblyInfo.GetTitle());
            Console.WriteLine(AssemblyInfo.GetCopyright());

            // Registers support for code pages like WIN1251
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            // Build up command line api
            var rootCommand = new RootCommand(description: "Tool for synchronizing ECF files with MAGELLAN")
            {
                CommandDefinitions.Export(),
                CommandDefinitions.InitExport(),
                CommandDefinitions.Import(),
                CommandDefinitions.InitImport(),
            };

            // Parse the incoming args and invoke the handler
            await rootCommand.InvokeAsync(args);
        }
    }
}
