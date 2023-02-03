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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;

public class LegacyIndexCqlTest extends SAITester
{
    @Test
    public void loadLegacyIndexAndRunCqlQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, int_value int, text_value text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();

        FileUtils.copySSTablesAndIndexes(directory.toJavaIOFile(), "aa");

        cfs.loadNewSSTables();

        UntypedResultSet resultSet = execute("SELECT * FROM %s WHERE int_value > 10 AND int_value < 20");
        assertEquals(9, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '10'");
        assertEquals(1, resultSet.size());

        for (int row = 100; row < 200; row++)
            execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", row, row, Integer.toString(row));

        resultSet = execute("SELECT * FROM %s WHERE int_value > 90 AND int_value < 110");
        assertEquals(19, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '10'");
        assertEquals(1, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '110'");
        assertEquals(1, resultSet.size());

        flush();

        resultSet = execute("SELECT * FROM %s WHERE int_value > 90 AND int_value < 110");
        assertEquals(19, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '10'");
        assertEquals(1, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '110'");
        assertEquals(1, resultSet.size());

        compact();
        waitForCompactions();

        resultSet = execute("SELECT * FROM %s WHERE int_value > 90 AND int_value < 110");
        assertEquals(19, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '10'");
        assertEquals(1, resultSet.size());

        resultSet = execute("SELECT * FROM %s WHERE text_value = '110'");
        assertEquals(1, resultSet.size());
    }
}
