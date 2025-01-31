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
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class FullAccordInteropMultiNodeTableWalkTest extends AccordInteropMultiNodeTableWalkBase
{
    public FullAccordInteropMultiNodeTableWalkTest()
    {
        super(TransactionalMode.full);
    }

    @Override
    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        cluster.setUncaughtExceptionsFilter(t -> {
            // There is a known issue with drop table and journal snapshots where the journal can't find the given table...
            // To make these tests stable need to ignore this error as it's unrelated to the test and is target to be fixed
            // directly.
            /*
Suppressed: java.lang.AssertionError: Unknown keyspace ks12
		at org.apache.cassandra.db.Keyspace.open(Keyspace.java:149)
		at org.apache.cassandra.db.Keyspace.openAndGetStoreIfExists(Keyspace.java:172)
		at org.apache.cassandra.service.accord.AccordDataStore.lambda$snapshot$2(AccordDataStore.java:104)
             */

            if (t instanceof AssertionError
                && t.getMessage() != null
                && t.getMessage().startsWith("Unknown keyspace ks"))
                return true;
            return false;
        });
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;

        builder.withExamples(50);
    }
}
