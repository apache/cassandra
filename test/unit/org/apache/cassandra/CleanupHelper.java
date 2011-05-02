/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CleanupHelper extends SchemaLoader
{
    private static Logger logger = LoggerFactory.getLogger(CleanupHelper.class);

    @BeforeClass
    public static void cleanupAndLeaveDirs() throws IOException
    {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
    }

    public static void cleanup() throws IOException
    {
        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }

        // clean up data directory which are stored as data directory/table/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }
    }

    public static void mkdirs()
    {
        try
        {
            DatabaseDescriptor.createAllDirectories();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void insertData(String keyspace, String columnFamily, int offset, int numberOfRows) throws IOException
    {
        for (int i = offset; i < offset + numberOfRows; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes("key" + i);
            RowMutation rowMutation = new RowMutation(keyspace, key);
            QueryPath path = new QueryPath(columnFamily, null, ByteBufferUtil.bytes("col" + i));

            rowMutation.add(path, ByteBufferUtil.bytes("val" + i), System.currentTimeMillis());
            rowMutation.applyUnsafe();
        }
    }

    /* usually used to populate the cache */
    protected void readData(String keyspace, String columnFamily, int offset, int numberOfRows) throws IOException
    {
        ColumnFamilyStore store = Table.open(keyspace).getColumnFamilyStore(columnFamily);
        for (int i = offset; i < offset + numberOfRows; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            QueryPath path = new QueryPath(columnFamily, null, ByteBufferUtil.bytes("col" + i));

            store.getColumnFamily(key, path, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        }
    }
}
