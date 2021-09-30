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
package org.apache.cassandra.triggers;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.*;

public class TriggersSchemaTest
{
    String ksName = "ks" + nanoTime();
    String cfName = "cf" + nanoTime();
    String triggerName = "trigger_" + nanoTime();
    String triggerClass = "org.apache.cassandra.triggers.NoSuchTrigger.class";

    @BeforeClass
    public static void beforeTest() throws ConfigurationException
    {
        SchemaLoader.loadSchema();
    }

    @Test
    public void newKsContainsCfWithTrigger() throws Exception
    {
        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        TableMetadata tm =
            CreateTableStatement.parse(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName)
                                .triggers(Triggers.of(td))
                                .build();

        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(tm));
        SchemaTestUtil.announceNewKeyspace(ksm);

        TableMetadata tm2 = Schema.instance.getTableMetadata(ksName, cfName);
        assertFalse(tm2.triggers.isEmpty());
        assertEquals(1, tm2.triggers.size());
        assertEquals(td, tm2.triggers.get(triggerName).get());
    }

    @Test
    public void addNewCfWithTriggerToKs() throws Exception
    {
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1));
        SchemaTestUtil.announceNewKeyspace(ksm);

        TableMetadata metadata =
            CreateTableStatement.parse(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName)
                                .triggers(Triggers.of(TriggerMetadata.create(triggerName, triggerClass)))
                                .build();

        SchemaTestUtil.announceNewTable(metadata);

        metadata = Schema.instance.getTableMetadata(ksName, cfName);
        assertFalse(metadata.triggers.isEmpty());
        assertEquals(1, metadata.triggers.size());
        assertEquals(TriggerMetadata.create(triggerName, triggerClass), metadata.triggers.get(triggerName).get());
    }

    @Test
    public void addTriggerToCf() throws Exception
    {
        TableMetadata tm1 =
            CreateTableStatement.parse(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName)
                                .build();
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(tm1));
        SchemaTestUtil.announceNewKeyspace(ksm);

        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        TableMetadata tm2 =
            Schema.instance
                  .getTableMetadata(ksName, cfName)
                  .unbuild()
                  .triggers(Triggers.of(td))
                  .build();
        SchemaTestUtil.announceTableUpdate(tm2);

        TableMetadata tm3 = Schema.instance.getTableMetadata(ksName, cfName);
        assertFalse(tm3.triggers.isEmpty());
        assertEquals(1, tm3.triggers.size());
        assertEquals(td, tm3.triggers.get(triggerName).get());
    }

    @Test
    public void removeTriggerFromCf() throws Exception
    {
        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        TableMetadata tm =
            CreateTableStatement.parse(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName)
                                .triggers(Triggers.of(td))
                                .build();
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(tm));
        SchemaTestUtil.announceNewKeyspace(ksm);

        TableMetadata tm1 = Schema.instance.getTableMetadata(ksName, cfName);
        TableMetadata tm2 =
            tm1.unbuild()
               .triggers(tm1.triggers.without(triggerName))
               .build();
        SchemaTestUtil.announceTableUpdate(tm2);

        TableMetadata tm3 = Schema.instance.getTableMetadata(ksName, cfName);
        assertTrue(tm3.triggers.isEmpty());
    }
}
