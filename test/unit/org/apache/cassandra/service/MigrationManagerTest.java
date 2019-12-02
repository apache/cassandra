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
package org.apache.cassandra.service;

import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.Tables;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MigrationManagerTest extends CQLTester
{
    @Test
    public void testEvolveSystemKeyspaceNew()
    {
        CFMetaData table = CFMetaData.compile("CREATE TABLE t (id int PRIMARY KEY)", "ks0");
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks0", KeyspaceParams.simple(1), Tables.of(table));

        Optional<Mutation> mutation = MigrationManager.evolveSystemKeyspace(keyspace, 0);
        assertTrue(mutation.isPresent());

        SchemaKeyspace.mergeSchema(singleton(mutation.get()));
        assertEquals(keyspace, Schema.instance.getKSMetaData("ks0"));
    }

    @Test
    public void testEvolveSystemKeyspaceExistsUpToDate()
    {
        CFMetaData table = CFMetaData.compile("CREATE TABLE t (id int PRIMARY KEY)", "ks1");
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks1", KeyspaceParams.simple(1), Tables.of(table));

        // create the keyspace, verify it's there
        SchemaKeyspace.mergeSchema(singleton(SchemaKeyspace.makeCreateKeyspaceMutation(keyspace, 0).build()));
        assertEquals(keyspace, Schema.instance.getKSMetaData("ks1"));

        Optional<Mutation> mutation = MigrationManager.evolveSystemKeyspace(keyspace, 0);
        assertFalse(mutation.isPresent());
    }

    @Test
    public void testEvolveSystemKeyspaceChanged()
    {
        CFMetaData table0 = CFMetaData.compile("CREATE TABLE t (id int PRIMARY KEY)", "ks2");
        KeyspaceMetadata keyspace0 = KeyspaceMetadata.create("ks2", KeyspaceParams.simple(1), Tables.of(table0));

        // create the keyspace, verify it's there
        SchemaKeyspace.mergeSchema(singleton(SchemaKeyspace.makeCreateKeyspaceMutation(keyspace0, 0).build()));
        assertEquals(keyspace0, Schema.instance.getKSMetaData("ks2"));

        CFMetaData table1 = table0.copy().comment("comment");
        KeyspaceMetadata keyspace1 = KeyspaceMetadata.create("ks2", KeyspaceParams.simple(1), Tables.of(table1));

        Optional<Mutation> mutation = MigrationManager.evolveSystemKeyspace(keyspace1, 1);
        assertTrue(mutation.isPresent());

        SchemaKeyspace.mergeSchema(singleton(mutation.get()));
        assertEquals(keyspace1, Schema.instance.getKSMetaData("ks2"));
    }
}