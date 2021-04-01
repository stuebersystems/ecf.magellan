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

using System;
using System.Collections.Generic;
using System.Linq;

namespace Ecf.Magellan
{
    public class SimpleCache
    {
        /// <summary>
        /// Incoming Ids from Ecf
        /// </summary>
        public string EcfId { get; set; }

        /// <summary>
        /// Id in MAGELLAN
        /// </summary>
        public int MagellanId { get; set; }

        public SimpleCache(string ecfId, int magellanId)
        {
            EcfId = ecfId;
            MagellanId = magellanId;
        }
    }
}
