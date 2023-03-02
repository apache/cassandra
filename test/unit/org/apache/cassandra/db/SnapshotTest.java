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

package org.apache.cassandra.db;

import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

public class SnapshotTest extends CQLTester
{
    @Test
    public void testEmptyTOC() throws Throwable
    {
        createTable("create table %s (id int primary key, k int)");
        execute("insert into %s (id, k) values (1,1)");
        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            File toc = sstable.descriptor.fileFor(Components.TOC);
            Files.write(toc.toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
        getCurrentColumnFamilyStore().snapshot("hello");
    }
}
