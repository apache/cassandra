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

package org.apache.cassandra.db.guardrails;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class GuardrailAllowUncompressedTablesTest extends GuardrailTester
{
    private void setGuardrail(boolean enabled)
    {
        guardrails().setUncompressedTablesEnabled(enabled);
    }

    /**
     * If the guardrail has been set, creating tables with compression disabled should work
     */
    @Test
    public void createSuccess()
    {
        setGuardrail(true);
        String table = createTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int) WITH compression={'enabled':false}", KEYSPACE, table));
        TableMetadata tmd = Schema.instance.getTableMetadata(KEYSPACE, table);
        Assert.assertFalse(tmd.params.compression.isEnabled());
    }

    /**
     * If the guardrail is false, creating tables with compression disabled should fail
     */
    @Test
    public void createFailure() throws Throwable
    {
        setGuardrail(false);
        String table = createTableName();
        assertFails(String.format("CREATE TABLE %s.%s (k int primary key, v int) WITH compression={'enabled': false}", KEYSPACE, table), "Uncompressed table is not allowed");
    }

    @Test
    public void alterSuccess()
    {
        setGuardrail(true);
        String table = createTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int)", KEYSPACE, table));
        schemaChange(String.format("ALTER TABLE %s.%s WITH compression = {'enabled': false}", KEYSPACE, table));
        TableMetadata tmd = Schema.instance.getTableMetadata(KEYSPACE, table);
        Assert.assertFalse(tmd.params.compression.isEnabled());
    }

    @Test
    public void alterFailure() throws Throwable
    {
        setGuardrail(false);
        String table = createTableName();
        schemaChange(String.format("CREATE TABLE %s.%s (k int primary key, v int)", KEYSPACE, table));
        assertFails(String.format("ALTER TABLE %s.%s WITH compression = {'enabled': false}", KEYSPACE, table), "Uncompressed table is not allowed");
    }
}
