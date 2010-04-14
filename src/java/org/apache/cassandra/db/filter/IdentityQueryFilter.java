package org.apache.cassandra.db.filter;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.SuperColumn;

class IdentityQueryFilter extends SliceQueryFilter
{
    /**
     * Only for use in testing; will read entire CF into memory.
     */
    public IdentityQueryFilter()
    {
        super(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, null, false, Integer.MAX_VALUE);
    }

    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // no filtering done, deliberately
        return superColumn;
    }
}
