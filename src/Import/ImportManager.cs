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

using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.Magellan
{
    public class ImportManager : CustomManager
    {
        private const string DefaultSearchPattern = "*.csv";
        private readonly int _schoolTermId;
        private readonly int _tenantId;

        public ImportManager(
            Configuration config,
            CancellationToken cancellationToken, 
            EventWaitHandle cancellationEvent)
            : base(config, cancellationToken, cancellationEvent)
        {
            _tenantId = _config.EcfImport.TenantId;
            _schoolTermId = _config.EcfImport.SchoolTermId;
        }

        public override async Task Execute()
        {
            await Task.CompletedTask;
        }
    }
}
