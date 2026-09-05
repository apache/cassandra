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

package org.apache.cassandra.test.microbench.sai;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class AbstractMemtableIndexBench extends CQLTester
{
    private static final int RANDOM_STRING_SIZE = 64 * 1024 * 1024;
    private static String keyspace;

    protected MemtableIndex memtableIndex;
    protected DecoratedKey[] partitionKeys;
    protected ByteBuffer[] terms;
    protected StorageAttachedIndex index;

    protected ColumnFamilyStore cfs;
    protected String table;
    private char[] randomChars = new char[RANDOM_STRING_SIZE];

    public void setup(int numberOfTerms, int rowsPerPartition)
    {
        setupServer();
        setupTableAndKeyspace();
        setupCfsAndIndex();
        setupPartitionKeys(numberOfTerms, rowsPerPartition);
        setupTerms(numberOfTerms);
    }

    public void setupServer()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setAutoSnapshot(false);
    }

    public void setupTableAndKeyspace()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s ( partition_id text, value_text text, PRIMARY KEY(partition_id)) with compression = {'enabled': false}",
                            "memtable_index");
        execute("use " + keyspace + ";");
    }

    public void setupCfsAndIndex()
    {
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                    StorageAttachedIndex.class.getCanonicalName());
        options.put("target", "value_text");

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("value_text_idx", IndexMetadata.Kind.CUSTOM, options);

        index = new StorageAttachedIndex(cfs, indexMetadata);
    }

    public void setupPartitionKeys(int numberOfTerms, int rowsPerPartition)
    {
        TableMetadata tableMetadata = cfs.metadata();

        int numberOfKeys = numberOfTerms / rowsPerPartition;

        partitionKeys = new DecoratedKey[numberOfKeys];
        for (int i = 0; i < numberOfKeys; i++)
            partitionKeys[i] = tableMetadata.partitioner.decorateKey(tableMetadata.partitionKeyType.fromString("partition_" + i));
    }

    public void setupTerms(int numberOfTerms)
    {
        Random random = new Random();
        for (int i = 0; i < RANDOM_STRING_SIZE; i++)
            randomChars[i] = (char)('a' + random.nextInt(26));

        int length = 64;
        terms = new ByteBuffer[numberOfTerms];
        for (int i = 0; i < numberOfTerms; i++)
            terms[i] = UTF8Type.instance.decompose(generateRandomString(random, length));
    }

    private String generateRandomString(Random random, int length)
    {
        return new String(randomChars, random.nextInt(RANDOM_STRING_SIZE - length), length);
    }
}
