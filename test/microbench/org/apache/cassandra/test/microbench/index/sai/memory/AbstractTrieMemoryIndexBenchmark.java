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

package org.apache.cassandra.test.microbench.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.LongConsumer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.UUIDGen;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

public abstract class AbstractTrieMemoryIndexBenchmark
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PARTITION_KEY = "key";
    private static final String STRING_COLUMN = "string";
    private static final String STRING_INDEX = "string_index";
    private static final String INTEGER_COLUMN = "integer";
    private static final String INTEGER_INDEX = "integer_index";
    private static final int RANDOM_STRING_SIZE = 64 * 1024 * 1024;

    private char[] randomChars = new char[RANDOM_STRING_SIZE];

    protected int randomSeed;

    protected ColumnContext stringContext;
    protected ColumnContext integerContext;

    protected MemoryIndex stringIndex;
    protected MemoryIndex integerIndex;

    protected ByteBuffer[] stringTerms;
    protected ByteBuffer[] integerTerms;
    protected DecoratedKey[] partitionKeys;

    @Setup(Level.Trial)
    public void initialiseConfig()
    {
        DatabaseDescriptor.daemonInitialization();
        Random random = new Random();
        randomSeed = random.nextInt();
        for (int i = 0; i < RANDOM_STRING_SIZE; i++)
        {
            randomChars[i] = (char)('a' + random.nextInt(26));
        }

        ColumnMetadata string = ColumnMetadata.regularColumn(KEYSPACE, TABLE, STRING_COLUMN, UTF8Type.instance);
        ColumnMetadata integer = ColumnMetadata.regularColumn(KEYSPACE, TABLE, INTEGER_COLUMN, Int32Type.instance);
        TableMetadata table = TableMetadata.builder(KEYSPACE, TABLE)
                .addPartitionKeyColumn(PARTITION_KEY, UTF8Type.instance)
                .addRegularColumn(STRING_COLUMN, UTF8Type.instance)
                .addRegularColumn(INTEGER_COLUMN, Int32Type.instance)
                .partitioner(Murmur3Partitioner.instance)
                .caching(CachingParams.CACHE_NOTHING)
                .build();

        Map<String, String> stringOptions = new HashMap<>();
        stringOptions.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        stringOptions.put(NonTokenizingOptions.CASE_SENSITIVE, "true");
        stringOptions.put(IndexTarget.TARGET_OPTION_NAME, STRING_COLUMN);

        Map<String, String> integerOptions = new HashMap<>();
        integerOptions.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        integerOptions.put(IndexTarget.TARGET_OPTION_NAME, INTEGER_COLUMN);

        IndexMetadata stringMetadata = IndexMetadata.fromSchemaMetadata(STRING_INDEX, IndexMetadata.Kind.CUSTOM, stringOptions);
        stringContext = new ColumnContext(table, stringMetadata);

        IndexMetadata integerMetadata = IndexMetadata.fromSchemaMetadata(INTEGER_INDEX, IndexMetadata.Kind.CUSTOM, integerOptions);
        integerContext = new ColumnContext(table, integerMetadata);
    }


    protected void initialiseColumnData(int numberOfTerms, int rowsPerPartition)
    {
        Random random = new Random(randomSeed);

        int numberOfKeys = numberOfTerms / rowsPerPartition;
        stringTerms = new ByteBuffer[numberOfTerms];
        integerTerms = new ByteBuffer[numberOfTerms];
        partitionKeys = new DecoratedKey[numberOfKeys];

        int length = 64;

        for (int i = 0; i < numberOfTerms; i++)
        {
            stringTerms[i] = UTF8Type.instance.decompose(generateRandomString(random, length));
            integerTerms[i] = Int32Type.instance.decompose(i);
        }

        for (int i = 0; i < numberOfKeys; i++)
        {
            partitionKeys[i] = Murmur3Partitioner.instance.decorateKey(UUIDType.instance.decompose(UUIDGen.getTimeUUID()));
        }
    }

    private String generateRandomString(Random random, int length)
    {
        return new String(randomChars, random.nextInt(RANDOM_STRING_SIZE - length), length);
    }
}
