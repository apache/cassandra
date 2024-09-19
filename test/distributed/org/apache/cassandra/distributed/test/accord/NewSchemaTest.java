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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.service.accord.AccordService;

import static java.util.function.UnaryOperator.identity;

public class NewSchemaTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(NewSchemaTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(identity(), 2);
    }

    @Test
    public void test()
    {
        for (int i = 0; i < 20; i++)
        {
            String ks = "ks" + i;
            String table = ks + ".tbl" + i;
            SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + ks + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
            SHARED_CLUSTER.schemaChange(String.format("CREATE TABLE %s (pk blob primary key) WITH transactional_mode='full'", table));
            SHARED_CLUSTER.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

            List<ByteBuffer> keys = tokensToKeys(tokens());

            read(table, keys).exec();
        }
    }

    private static Query read(String table, List<ByteBuffer> keys)
    {
        assert !keys.isEmpty();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++)
            sb.append("let row").append(i).append(" = (select * from ").append(table).append(" where pk = ?);\n");
        sb.append("SELECT row0.pk;");
        return new Query(sb.toString(), keys.toArray());
    }

    private static class Query
    {
        final String cql;
        final Object[] binds;

        private Query(String cql, Object[] binds)
        {
            this.cql = cql;
            this.binds = binds;
        }

        SimpleQueryResult exec()
        {
            return executeWithRetry(SHARED_CLUSTER, cql, binds);
        }
    }
}
