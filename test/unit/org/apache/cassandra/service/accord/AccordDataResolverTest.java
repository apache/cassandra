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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters.Filter;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;

public class AccordDataResolverTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(org.apache.cassandra.distributed.test.accord.AccordCQLTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
//        AccordTestBase.setupCluster(builder -> builder, 2);
        AccordTestBase.setupCluster(builder -> builder.appendConfig(config -> config.set("lwt_strategy", "accord")), 2);
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }

    @Test
    public void testReadRepair() throws Exception
    {
        test(cluster -> {
            Filter mutationFilter = cluster.filters().verbs(Verb.MUTATION_REQ.id).drop().on();
            cluster.filters().verbs(Verb.HINT_REQ.id, Verb.HINT_RSP.id).drop().on();
            cluster.coordinator(1).execute("INSERT INTO " + currentTable + " (k, c, v) VALUES (1, 1, 1);", ConsistencyLevel.ONE);
            mutationFilter.off();
            Filter blockNodeOneReads = cluster.filters().verbs(Verb.READ_REQ.id).to(1).drop().on();
            assertThat(cluster.coordinator(2).executeWithResult("SELECT * FROM " + currentTable + " WHERE k = 1 AND c = 1;", ConsistencyLevel.ONE))
                .isEmpty();
            blockNodeOneReads.off();
            // Should perform read repair
            Object[][] result = cluster.coordinator(1).execute("SELECT * FROM " + currentTable + " WHERE k = 1 AND c = 1;", ConsistencyLevel.SERIAL);
            blockNodeOneReads.on();
            // Side effect of the read repair should be visible now
            assertThat(cluster.coordinator(2).executeWithResult("SELECT * FROM " + currentTable + " WHERE k = 1 AND c = 1;", ConsistencyLevel.ONE))
                .isEqualTo(1, 1, 1);
        });
    }
}
