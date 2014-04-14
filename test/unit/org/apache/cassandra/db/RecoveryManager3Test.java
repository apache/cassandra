package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.File;
import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;

public class RecoveryManager3Test extends SchemaLoader
{
    @Test
    public void testMissingHeader() throws IOException
    {
        Keyspace keyspace1 = Keyspace.open("Keyspace1");
        Keyspace keyspace2 = Keyspace.open("Keyspace2");

        Mutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation("Keyspace1", dk.getKey(), cf);
        rm.apply();

        cf = ArrayBackedSortedColumns.factory.create("Keyspace2", "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm = new Mutation("Keyspace2", dk.getKey(), cf);
        rm.apply();

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        // nuke the header
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
        {
            if (file.getName().endsWith(".header"))
                FileUtils.deleteWithConfirm(file);
        }

        CommitLog.instance.resetUnsafe(); // disassociate segments from live CL
        CommitLog.instance.recover();

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard3"), "col2");
    }
}
