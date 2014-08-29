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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.triggers.ITrigger;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CreateTriggerStatementTest extends CQLTester
{
    @Test
    public void testCreateTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);
        execute("CREATE TRIGGER trigger_2 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_2", TestTrigger.class);
        assertInvalid("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3", TestTrigger.class);
    }

    @Test
    public void testCreateTriggerIfNotExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER IF NOT EXISTS trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);
    }

    @Test
    public void testDropTrigger() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("DROP TRIGGER trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        assertInvalid("DROP TRIGGER trigger_2 ON %s");
        
        execute("CREATE TRIGGER \"Trigger 3\" ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("Trigger 3", TestTrigger.class);

        execute("DROP TRIGGER \"Trigger 3\" ON %s");
        assertTriggerDoesNotExists("Trigger 3", TestTrigger.class);
    }

    @Test
    public void testDropTriggerIfExists() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);

        execute("CREATE TRIGGER trigger_1 ON %s USING '" + TestTrigger.class.getName() + "'");
        assertTriggerExists("trigger_1", TestTrigger.class);

        execute("DROP TRIGGER IF EXISTS trigger_1 ON %s");
        assertTriggerDoesNotExists("trigger_1", TestTrigger.class);
    }

    private void assertTriggerExists(String name, Class<?> clazz)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), currentTable()).copy();
        assertTrue("the trigger does not exist", cfm.containsTriggerDefinition(TriggerDefinition.create(name,
                clazz.getName())));
    }

    private void assertTriggerDoesNotExists(String name, Class<?> clazz)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), currentTable()).copy();
        assertFalse("the trigger exists", cfm.containsTriggerDefinition(TriggerDefinition.create(name,
                clazz.getName())));
    }

    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            return Collections.emptyList();
        }
    }
}
