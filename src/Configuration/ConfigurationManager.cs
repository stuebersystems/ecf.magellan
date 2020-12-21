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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class ConfigurationManager
    {
        private const string EmbeddedNodeName = "Magellan";

        public static async Task<Configuration> LoadFromFile(string fileName, CancellationToken cancellationToken = default)
        {
            var jsonDoc = await JsonDocumentUtils.ParseFileAsync(fileName, cancellationToken);

            if (jsonDoc.RootElement.TryGetProperty(EmbeddedNodeName, out var jsonElement))
            {
                return JsonSerializer.Deserialize<Configuration>(jsonElement.GetRawText());
            }
            else
            {
                throw new InvalidOperationException($"No configuration for \"{EmbeddedNodeName}\" found in file \"{fileName}\").");
            }
        }

        public static async Task InitConfig(string fileName, string templateFileName, CancellationToken cancellationToken = default)
        {
            // Merge JSON configuration template with existing JSON configuration
            await JsonDocumentUtils.MergeFilesAsync(fileName, templateFileName, cancellationToken);

            // Report status
            Console.WriteLine();
            Console.WriteLine($"[Config] Configuration template created in file \"{fileName}\"");
        }
    }
}