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

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;

import static org.junit.Assert.assertEquals;

public class EmptyMemtableFlushTest extends SAITester
{
    @Test
    public void numericIndexTest() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val1 int, val2 int)");
        IndexContext val1IndexContext = createIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'"), Int32Type.instance);
        IndexContext val2IndexContext = createIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'"), Int32Type.instance);
        execute("INSERT INTO %s (id, val1, val2) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (id, val2) VALUES (1, 1)");
        execute("DELETE FROM %s WHERE id = 0");
        flush();
        // After this we should have only 1 set of index files but 2 completion markers
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.KD_TREE, val1IndexContext).size());
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.KD_TREE_POSTING_LISTS, val1IndexContext).size());
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.META, val1IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.COLUMN_COMPLETION_MARKER, val1IndexContext).size());

        assertEquals(1, componentFiles(indexFiles(), IndexComponent.KD_TREE, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.KD_TREE_POSTING_LISTS, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.META, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.COLUMN_COMPLETION_MARKER, val2IndexContext).size());

        assertEquals(0, execute("SELECT * from %s WHERE val1 = 0").size());
        assertEquals(1, execute("SELECT * from %s WHERE val2 = 1").size());
    }

    @Test
    public void literalIndexTest() throws Throwable
    {
        requireNetwork();
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val1 text, val2 text)");
        IndexContext val1IndexContext = createIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'"), UTF8Type.instance);
        IndexContext val2IndexContext = createIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'"), UTF8Type.instance);
        execute("INSERT INTO %s (id, val1, val2) VALUES (0, '0', '0')");
        execute("INSERT INTO %s (id, val2) VALUES (1, '1')");
        execute("DELETE FROM %s WHERE id = 0");
        flush();
        // After this we should have only 1 set of index files but 2 completion markers
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.TERMS_DATA, val1IndexContext).size());
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.POSTING_LISTS, val1IndexContext).size());
        assertEquals(0, componentFiles(indexFiles(), IndexComponent.META, val1IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.COLUMN_COMPLETION_MARKER, val1IndexContext).size());

        assertEquals(1, componentFiles(indexFiles(), IndexComponent.TERMS_DATA, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.POSTING_LISTS, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.META, val2IndexContext).size());
        assertEquals(1, componentFiles(indexFiles(), IndexComponent.COLUMN_COMPLETION_MARKER, val2IndexContext).size());

        assertEquals(0, execute("SELECT * from %s WHERE val1 = '0'").size());
        assertEquals(1, execute("SELECT * from %s WHERE val2 = '1'").size());
    }
}
