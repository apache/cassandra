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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.service.MigrationManager;

import static org.junit.Assert.*;

public class TriggersSchemaTest
{
    String ksName = "ks" + System.nanoTime();
    String cfName = "cf" + System.nanoTime();
    String triggerName = "trigger_" + System.nanoTime();
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
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        cfm1.triggers(cfm1.getTriggers().with(td));
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm2.getTriggers().isEmpty());
        assertEquals(1, cfm2.getTriggers().size());
        assertEquals(td, cfm2.getTriggers().get(triggerName).get());
    }

    @Test
    public void addNewCfWithTriggerToKs() throws Exception
    {
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        cfm1.triggers(cfm1.getTriggers().with(td));

        MigrationManager.announceNewColumnFamily(cfm1);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm2.getTriggers().isEmpty());
        assertEquals(1, cfm2.getTriggers().size());
        assertEquals(td, cfm2.getTriggers().get(triggerName).get());
    }

    @Test
    public void addTriggerToCf() throws Exception
    {
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        cfm2.triggers(cfm2.getTriggers().with(td));
        MigrationManager.announceColumnFamilyUpdate(cfm2, false);

        CFMetaData cfm3 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm3.getTriggers().isEmpty());
        assertEquals(1, cfm3.getTriggers().size());
        assertEquals(td, cfm3.getTriggers().get(triggerName).get());
    }

    @Test
    public void removeTriggerFromCf() throws Exception
    {
        TriggerMetadata td = TriggerMetadata.create(triggerName, triggerClass);
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        cfm1.triggers(cfm1.getTriggers().with(td));
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(1), Tables.of(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        cfm2.triggers(cfm2.getTriggers().without(triggerName));
        MigrationManager.announceColumnFamilyUpdate(cfm2, false);

        CFMetaData cfm3 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        assertTrue(cfm3.getTriggers().isEmpty());
    }
}
