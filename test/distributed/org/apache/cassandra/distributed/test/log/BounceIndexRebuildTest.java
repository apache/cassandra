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

package org.apache.cassandra.distributed.test.log;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;

public class BounceIndexRebuildTest extends TestBaseImpl
{
    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int)"));
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, x) values (?, ?)"), ConsistencyLevel.ALL, i, i);

            cluster.schemaChange(withKeyspace("create index idx on %s.tbl (x)"));
            Object[][] res = cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where x=5"), ConsistencyLevel.ALL);
            assert res.length > 0;
            String patternLegacyBuild = "Index build of idx complete";
            int preBounceLegacyBuilds = cluster.get(1).logs().grep(patternLegacyBuild).getResult().size();

            final String patternSaiValidation = "Validating per-column index components for distributed_test_keyspace.idx";
            int preBounceSaiValidations = cluster.get(1).logs().grep(patternSaiValidation).getResult().size();

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            // Make sure legacy index does not rebuild on restart
            assertEquals(preBounceLegacyBuilds, cluster.get(1).logs().grep(patternLegacyBuild).getResult().size());
            // If we are using SAI, we want the index to validate rather than build
            if (preBounceLegacyBuilds == 0)
                Assert.assertTrue(cluster.get(1).logs().grep(patternSaiValidation).getResult().size() > preBounceSaiValidations);

            res = cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where x=5"), ConsistencyLevel.ALL);
            assert res.length > 0;
        }
    }
}
