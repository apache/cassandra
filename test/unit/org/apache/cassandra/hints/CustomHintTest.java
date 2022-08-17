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

package org.apache.cassandra.hints;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_HINTS_HANDLER;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.junit.Assert.assertTrue;

public class CustomHintTest
{
    private static final String KEYSPACE = "custom_hint_test";
    private static final String TABLE = "table";

    private static boolean customVerbCalled = false;

    @BeforeClass
    public static void defineSchema()
    {
        CUSTOM_HINTS_HANDLER.setString(CustomHintVerbHandler.class.getName());
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @AfterClass
    public static void resetCustom()
    {
        System.clearProperty(CUSTOM_HINTS_HANDLER.getKey());
    }

    @After
    public void resetAfter()
    {
        customVerbCalled = false;
    }

    @Test
    public void testChangedTopology() throws Exception
    {
        Hint hint = createHint();
        UUID localId = StorageService.instance.getLocalHostUUID();
        HintMessage message = new HintMessage(localId, hint);

        HINT_REQ.handler().doVerb(Message.out(HINT_REQ, message));
        assertTrue(customVerbCalled);
    }

    private Hint createHint()
    {
        long now = System.currentTimeMillis();
        DecoratedKey dkey = dk(String.valueOf(1));
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
        builder.row("column0").add("val", "value0");
        
        return Hint.create(builder.buildAsMutation(), now);
    }

    public static class CustomHintVerbHandler implements IVerbHandler<HintMessage>
    {
        @Override
        public void doVerb(Message<HintMessage> message)
        {
            customVerbCalled = true;
        }
    }
}
