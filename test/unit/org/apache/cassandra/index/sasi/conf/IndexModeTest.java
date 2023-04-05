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
package org.apache.cassandra.index.sasi.conf;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder.Mode;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;


public class IndexModeTest
{

    private final TableMetadata cfm = TableMetadata.builder("ks", "cf")
                        .addPartitionKeyColumn("pkey", AsciiType.instance)
                        .build();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void test_notIndexed()
    {
        Assert.assertEquals(IndexMode.NOT_INDEXED, IndexMode.getMode(null, (Map)null));
        Assert.assertEquals(IndexMode.NOT_INDEXED, IndexMode.getMode(null, Collections.EMPTY_MAP));
    }

    @Test(expected = ConfigurationException.class)
    public void test_bad_mode_option()
    {
        IndexMode.getMode(null, Collections.singletonMap("mode", "invalid"));
    }

    @Test
    public void test_asciiType()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), AsciiType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("something", "nothing"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertTrue(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_asciiType_notLiteral()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), AsciiType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("is_literal", "false"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_asciiType_errLiteral()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), AsciiType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("is_literal", "junk"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_asciiType_analyzed()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), AsciiType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("analyzed", "true"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertTrue(result.isAnalyzed);
        Assert.assertTrue(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_utf8Type()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), UTF8Type.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("something", "nothing"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertTrue(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("something", "nothing"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_isLiteral()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("is_literal", "true"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertTrue(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_errLiteral()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("is_literal", "junk"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_analyzed()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("analyzed", "true"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertTrue(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_analyzer()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("analyzer_class", "java.lang.Object"));
        Assert.assertEquals(Object.class, result.analyzerClass);
        Assert.assertTrue(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_analyzer_unanalyzed()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, ImmutableMap.of("analyzer_class", "java.lang.Object",
                                                                 "analyzed", "false"));

        Assert.assertEquals(Object.class, result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals((long)(1073741824 * 0.15), result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }

    @Test
    public void test_bytesType_maxCompactionFlushMemoryInBytes()
    {
        ColumnMetadata cd = ColumnMetadata.regularColumn(cfm, ByteBufferUtil.bytes("TestColumnMetadata"), BytesType.instance);

        IndexMode result = IndexMode.getMode(cd, Collections.singletonMap("max_compaction_flush_memory_in_mb", "1"));
        Assert.assertNull(result.analyzerClass);
        Assert.assertFalse(result.isAnalyzed);
        Assert.assertFalse(result.isLiteral);
        Assert.assertEquals(1048576L, result.maxCompactionFlushMemoryInBytes);
        Assert.assertEquals(Mode.PREFIX, result.mode);
    }
}
