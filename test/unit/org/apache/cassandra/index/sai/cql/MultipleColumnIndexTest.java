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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class MultipleColumnIndexTest extends SAITester
{
    // Note: Full testing of multiple map index types is done in the
    // types/collections/maps/MultiMap*Test tests
    // This is just testing that the indexes can be created
    @Test
    public void canCreateMultipleMapIndexesOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value map<int,int>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(value)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(value)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ENTRIES(value)) USING 'StorageAttachedIndex'");
    }

    @Test
    public void cannotHaveMultipleLiteralIndexesWithDifferentOptions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : true }");
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : false }"))
                .isInstanceOf(RuntimeException.class).hasCauseInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void indexNamedAsColumnWillCoExistWithGeneratedIndexNames() throws Throwable
    {
        createTable("CREATE TABLE %s(id int PRIMARY KEY, text_map map<text, text>)");
        execute("INSERT INTO %s(id, text_map) values (1, {'k1':'v1', 'k2':'v2'})");
        execute("INSERT INTO %s(id, text_map) values (2, {'k1':'v1', 'k3':'v3'})");
        execute("INSERT INTO %s(id, text_map) values (3, {'k4':'v4', 'k5':'v5'})");

        flush();

        createIndex("CREATE CUSTOM INDEX text_map ON %s(keys(text_map)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(values(text_map)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(text_map)) USING 'StorageAttachedIndex'");

        waitForIndexQueryable();

        assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map['k2'] = 'v2'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS 'v1'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k2'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS 'v1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k2' AND text_map CONTAINS 'v1'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE text_map['k1'] = 'v1' AND text_map CONTAINS KEY 'k1' AND text_map CONTAINS KEY 'k4'").size());
    }
}
