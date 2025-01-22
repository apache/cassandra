/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.cql3;

import accord.utils.Property;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class AccordInteropSingleNodeTokenConflictTest extends SingleNodeTokenConflictTest
{
    public AccordInteropSingleNodeTokenConflictTest()
    {
        super(TransactionalMode.full);
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        //TODO (January): checkpoint a failing seed to debug later... accord returns incorrect data for the following SQL
        // 97: SELECT * FROM ks1.tbl WHERE token(pk0) BETWEEN token([8473585318424753772, 1213177836815110536]) AND token([-7320550072265110851, 4691861474352962931]); -- token BETWEEN, rc=-1, start token=-9116738905031522785, end token=1077669339564852589, on node1, fetch size 1
        builder.withSeed(3448341964809595261L);
    }
}
