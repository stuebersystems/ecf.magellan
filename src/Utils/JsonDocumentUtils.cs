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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public static class JsonDocumentUtils
    {
        public static async Task MergeFilesAsync(string fileName, string templateFileName, CancellationToken cancellationToken = default)
        {
            if (File.Exists(templateFileName))
            {
                if (File.Exists(fileName))
                {
                    using var jsonInputDoc = await ParseFileAsync(fileName, cancellationToken);
                    using var jsonTemplateDoc = await ParseFileAsync(templateFileName, cancellationToken);

                    using var jsonOutputStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None);
                    using var jsonOutputWriter = new Utf8JsonWriter(jsonOutputStream, new JsonWriterOptions { Indented = true });

                    JsonElement jsonInputRoot = jsonInputDoc.RootElement;
                    JsonElement jsonTemplateRoot = jsonTemplateDoc.RootElement;

                    if (jsonInputRoot.ValueKind != JsonValueKind.Object)
                    {
                        throw new InvalidOperationException($"The original JSON document to merge new content into must be an object type. Instead it is {jsonInputRoot.ValueKind}.");
                    }

                    if (jsonInputRoot.ValueKind != jsonTemplateRoot.ValueKind)
                    {
                        jsonInputRoot.WriteTo(jsonOutputWriter);
                    }
                    else
                    {
                        MergeObjects(jsonOutputWriter, jsonInputRoot, jsonTemplateRoot);
                    }
                }
                else
                {
                    File.Copy(templateFileName, fileName);
                }
            }
            else
            {
                throw new FileNotFoundException($"Template file \"{templateFileName}\") does not exists.");
            }
        }

        public static async Task<JsonDocument> ParseFileAsync(string fileName, CancellationToken cancellationToken = default)
        {
            if (File.Exists(fileName))
            {
                using var fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);

                return await JsonDocument.ParseAsync(fileStream, default, cancellationToken);
            }
            else
            {
                throw new FileNotFoundException($"File \"{fileName}\") does not exists.");
            }
        }

        private static void MergeObjects(Utf8JsonWriter jsonWriter, JsonElement element1, JsonElement element2)
        {
            Debug.Assert(element1.ValueKind == JsonValueKind.Object);
            Debug.Assert(element2.ValueKind == JsonValueKind.Object);

            jsonWriter.WriteStartObject();

            foreach (JsonProperty property in element1.EnumerateObject())
            {
                string propertyName = property.Name;

                JsonValueKind newValueKind;

                if (element2.TryGetProperty(propertyName, out JsonElement newValue) && (newValueKind = newValue.ValueKind) != JsonValueKind.Null)
                {
                    jsonWriter.WritePropertyName(propertyName);

                    JsonElement originalValue = property.Value;
                    JsonValueKind originalValueKind = originalValue.ValueKind;

                    if (newValueKind == JsonValueKind.Object && originalValueKind == JsonValueKind.Object)
                    {
                        MergeObjects(jsonWriter, originalValue, newValue); 
                    }
                }
                else
                {
                    property.WriteTo(jsonWriter);
                }
            }

            foreach (JsonProperty property in element2.EnumerateObject())
            {
                if (!element1.TryGetProperty(property.Name, out _))
                {
                    property.WriteTo(jsonWriter);
                }
            }

            jsonWriter.WriteEndObject();
        }
    }
}