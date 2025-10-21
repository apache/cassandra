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

package org.apache.cassandra.db.compression;

import java.util.function.Consumer;
import javax.management.openmbean.CompositeData;

import org.junit.Test;

import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CompressionDictionaryHelper;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompressionDictionaryDataObjectTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tb";
    private static final CompressionDictionary COMPRESSION_DICTIONARY = CompressionDictionaryHelper.INSTANCE.trainDictionary(KEYSPACE, TABLE);
    private static final CompressionDictionaryDataObject VALID_OBJECT = createValidObject();

    @Test
    public void testConversionOfCompressionDictionaryDataObjectToCompositeDataAndBack()
    {
        CompositeData compositeData = CompressionDictionaryDetailsTabularData.fromCompressionDictionaryDataObject(VALID_OBJECT);
        CompressionDictionaryDataObject dataObject = CompressionDictionaryDetailsTabularData.fromCompositeData(compositeData);

        assertEquals(VALID_OBJECT.keyspace, dataObject.keyspace);
        assertEquals(VALID_OBJECT.table, dataObject.table);
        assertEquals(VALID_OBJECT.dictId, dataObject.dictId);
        assertArrayEquals(VALID_OBJECT.dict, dataObject.dict);
        assertEquals(VALID_OBJECT.kind, dataObject.kind);
        assertEquals(VALID_OBJECT.dictChecksum, dataObject.dictChecksum);
        assertEquals(VALID_OBJECT.dictLength, dataObject.dictLength);
    }

    @Test
    public void testConversionOfCompressionDictionaryToDataObject()
    {
        CompositeData compositeData = CompressionDictionaryDetailsTabularData.fromCompressionDictionary(KEYSPACE, TABLE, COMPRESSION_DICTIONARY);
        CompressionDictionaryDataObject dataObject = CompressionDictionaryDetailsTabularData.fromCompositeData(compositeData);

        assertEquals(KEYSPACE, dataObject.keyspace);
        assertEquals(TABLE, dataObject.table);
        assertEquals(COMPRESSION_DICTIONARY.dictId().id, dataObject.dictId);
        assertArrayEquals(COMPRESSION_DICTIONARY.rawDictionary(), dataObject.dict);
        assertEquals(COMPRESSION_DICTIONARY.kind().name(), dataObject.kind);
        assertEquals(COMPRESSION_DICTIONARY.checksum(), dataObject.dictChecksum);
        assertEquals(COMPRESSION_DICTIONARY.rawDictionary().length, dataObject.dictLength);
    }

    @Test
    public void testConversionOfLightweightDictionaryToCompositeData()
    {
        LightweightCompressionDictionary lightweight = new LightweightCompressionDictionary(KEYSPACE,
                                                                                            TABLE,
                                                                                            COMPRESSION_DICTIONARY.dictId(),
                                                                                            COMPRESSION_DICTIONARY.checksum(),
                                                                                            COMPRESSION_DICTIONARY.rawDictionary().length);

        CompositeData compositeData = CompressionDictionaryDetailsTabularData.fromLightweightCompressionDictionary(lightweight);

        assertEquals(KEYSPACE, compositeData.get(CompressionDictionaryDetailsTabularData.KEYSPACE_NAME));
        assertEquals(TABLE, compositeData.get(CompressionDictionaryDetailsTabularData.TABLE_NAME));
        assertEquals(COMPRESSION_DICTIONARY.dictId().id, compositeData.get(CompressionDictionaryDetailsTabularData.DICT_ID_NAME));
        assertNull(compositeData.get(CompressionDictionaryDetailsTabularData.DICT_NAME));
        assertEquals(COMPRESSION_DICTIONARY.dictId().kind.name(), compositeData.get(CompressionDictionaryDetailsTabularData.KIND_NAME));
        assertEquals(COMPRESSION_DICTIONARY.checksum(), compositeData.get(CompressionDictionaryDetailsTabularData.CHECKSUM_NAME));
        assertEquals(COMPRESSION_DICTIONARY.rawDictionary().length, compositeData.get(CompressionDictionaryDetailsTabularData.SIZE_NAME));
    }

    @Test
    public void testValidation()
    {
        assertInvalid(modifier -> modifier.withKeyspace(null), "Keyspace not specified.");
        assertInvalid(modifier -> modifier.withTable(null), "Table not specified.");
        assertInvalid(modifier -> modifier.withDictId(-1), "Provided dictionary id must be positive but it is '-1'.");
        assertInvalid(modifier -> modifier.withDict(null), "Provided dictionary byte array is null or empty.");
        assertInvalid(modifier -> modifier.withDict(new byte[0]), "Provided dictionary byte array is null or empty.");
        assertInvalid(modifier -> modifier.withDict(new byte[((int) FileUtils.ONE_MIB) + 1]),
                      "Imported dictionary can not be larger than 1048576 bytes, but it is 1048577 bytes.");
        assertInvalid(modifier -> modifier.withKind(null), "Provided kind is null.");
        assertInvalid(modifier -> modifier.withKind("NONSENSE"), "There is no such dictionary kind like 'NONSENSE'. Available kinds: [ZSTD]");
        assertInvalid(modifier -> modifier.withDictLength(0), "Size has to be strictly positive number, it is '0'.");
        assertInvalid(modifier -> modifier.withDictLength(-10), "Size has to be strictly positive number, it is '-10'.");
        assertInvalid(modifier -> modifier.withDictLength(5),
                      "The length of the provided dictionary array (" + VALID_OBJECT.dictLength +
                      ") is not equal to provided length value (5).");
        assertInvalid(builder -> builder.withDictChecksum(VALID_OBJECT.dictChecksum + 1),
                      "Computed checksum of dictionary to import (" + VALID_OBJECT.dictChecksum +
                      ") is different from checksum specified on input (" + (VALID_OBJECT.dictChecksum + 1) + ").");
    }

    private void assertInvalid(Consumer<DataObjectModifier> action, String expectedExceptionMessage)
    {
        DataObjectModifier builder = new DataObjectModifier(VALID_OBJECT);
        action.accept(builder);

        assertThatThrownBy(builder::build)
        .hasMessageContaining(expectedExceptionMessage)
        .isInstanceOf(IllegalArgumentException.class);
    }

    private static CompressionDictionaryDataObject createValidObject()
    {
        return new CompressionDictionaryDataObject("ks",
                                                   "tb",
                                                   123,
                                                   COMPRESSION_DICTIONARY.rawDictionary(),
                                                   CompressionDictionary.Kind.ZSTD.name(),
                                                   CompressionDictionary.calculateChecksum((byte) CompressionDictionary.Kind.ZSTD.ordinal(),
                                                                                           123,
                                                                                           COMPRESSION_DICTIONARY.rawDictionary()),
                                                   COMPRESSION_DICTIONARY.rawDictionary().length);
    }

    private static class DataObjectModifier
    {
        private String keyspace;
        private String table;
        private long dictId;
        private byte[] dict;
        private String kind;
        private int dictChecksum;
        private int dictLength;

        public DataObjectModifier(CompressionDictionaryDataObject from)
        {
            withKeyspace(from.keyspace);
            withTable(from.table);
            withDictId(from.dictId);
            withDict(from.dict);
            withKind(from.kind);
            withDictChecksum(from.dictChecksum);
            withDictLength(from.dictLength);
        }

        public CompressionDictionaryDataObject build()
        {
            return new CompressionDictionaryDataObject(keyspace, table, dictId, dict, kind, dictChecksum, dictLength);
        }

        public DataObjectModifier withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public DataObjectModifier withTable(String table)
        {
            this.table = table;
            return this;
        }

        public DataObjectModifier withDictId(long dictId)
        {
            this.dictId = dictId;
            return this;
        }

        public DataObjectModifier withDict(byte[] dict)
        {
            this.dict = dict;
            return this;
        }

        public DataObjectModifier withKind(String kind)
        {
            this.kind = kind;
            return this;
        }

        public DataObjectModifier withDictChecksum(int dictChecksum)
        {
            this.dictChecksum = dictChecksum;
            return this;
        }

        public DataObjectModifier withDictLength(int dictLength)
        {
            this.dictLength = dictLength;
            return this;
        }
    }
}
