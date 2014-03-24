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

import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TriggersSchemaTest extends SchemaLoader
{
    String ksName = "ks" + System.nanoTime();
    String cfName = "cf" + System.nanoTime();
    String triggerName = "trigger_" + System.nanoTime();
    String triggerClass = "org.apache.cassandra.triggers.NoSuchTrigger.class";

    @Test
    public void newKsContainsCfWithTrigger() throws Exception
    {
        TriggerDefinition td = TriggerDefinition.create(triggerName, triggerClass);
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        cfm1.addTriggerDefinition(td);
        KSMetaData ksm = KSMetaData.newKeyspace(ksName,
                                                SimpleStrategy.class,
                                                Collections.singletonMap("replication_factor", "1"),
                                                true,
                                                Collections.singletonList(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm2.getTriggers().isEmpty());
        assertEquals(1, cfm2.getTriggers().size());
        assertEquals(td, cfm2.getTriggers().get(triggerName));
    }

    @Test
    public void addNewCfWithTriggerToKs() throws Exception
    {
        KSMetaData ksm = KSMetaData.newKeyspace(ksName,
                                                SimpleStrategy.class,
                                                Collections.singletonMap("replication_factor", "1"),
                                                true,
                                                Collections.EMPTY_LIST);
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        TriggerDefinition td = TriggerDefinition.create(triggerName, triggerClass);
        cfm1.addTriggerDefinition(td);

        MigrationManager.announceNewColumnFamily(cfm1);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm2.getTriggers().isEmpty());
        assertEquals(1, cfm2.getTriggers().size());
        assertEquals(td, cfm2.getTriggers().get(triggerName));
    }

    @Test
    public void addTriggerToCf() throws Exception
    {
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        KSMetaData ksm = KSMetaData.newKeyspace(ksName,
                                                SimpleStrategy.class,
                                                Collections.singletonMap("replication_factor", "1"),
                                                true,
                                                Collections.singletonList(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        TriggerDefinition td = TriggerDefinition.create(triggerName, triggerClass);
        cfm2.addTriggerDefinition(td);
        MigrationManager.announceColumnFamilyUpdate(cfm2, false);

        CFMetaData cfm3 = Schema.instance.getCFMetaData(ksName, cfName);
        assertFalse(cfm3.getTriggers().isEmpty());
        assertEquals(1, cfm3.getTriggers().size());
        assertEquals(td, cfm3.getTriggers().get(triggerName));
    }

    @Test
    public void removeTriggerFromCf() throws Exception
    {
        TriggerDefinition td = TriggerDefinition.create(triggerName, triggerClass);
        CFMetaData cfm1 = CFMetaData.compile(String.format("CREATE TABLE %s (k int PRIMARY KEY, v int)", cfName), ksName);
        cfm1.addTriggerDefinition(td);
        KSMetaData ksm = KSMetaData.newKeyspace(ksName,
                                                SimpleStrategy.class,
                                                Collections.singletonMap("replication_factor", "1"),
                                                true,
                                                Collections.singletonList(cfm1));
        MigrationManager.announceNewKeyspace(ksm);

        CFMetaData cfm2 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        cfm2.removeTrigger(triggerName);
        MigrationManager.announceColumnFamilyUpdate(cfm2, false);

        CFMetaData cfm3 = Schema.instance.getCFMetaData(ksName, cfName).copy();
        assertTrue(cfm3.getTriggers().isEmpty());
    }
}
