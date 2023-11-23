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

package org.apache.cassandra.index.sai.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.SAITester.getRandom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexTermTypeTest
{
    @Test
    public void testSimpleType()
    {
        for (CQL3Type cql3Type : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> type = cql3Type.getType();
            AbstractType<?> reversedType = ReversedType.getInstance(type);
            IndexTermType indexTermType = SAITester.createIndexTermType(type);
            IndexTermType reversedIndexTermType = SAITester.createIndexTermType(reversedType);
            boolean isUTF8OrAscii = cql3Type == CQL3Type.Native.ASCII || cql3Type == CQL3Type.Native.TEXT ||
                                    cql3Type == CQL3Type.Native.VARCHAR;
            boolean isLiteral = cql3Type == CQL3Type.Native.ASCII || cql3Type == CQL3Type.Native.TEXT ||
                                cql3Type == CQL3Type.Native.VARCHAR || cql3Type == CQL3Type.Native.BOOLEAN;
            assertEquals(isLiteral, indexTermType.isLiteral());
            assertEquals(indexTermType.isLiteral(), reversedIndexTermType.isLiteral());
            assertEquals(isUTF8OrAscii, indexTermType.isString());
            assertEquals(indexTermType.isString(), reversedIndexTermType.isString());
        }
    }

    @Test
    public void testMapType()
    {
        for (CQL3Type keyCql3Type : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> keyType = keyCql3Type.getType();

            testCollectionType((valueType, multiCell) -> MapType.getInstance(keyType, valueType, multiCell),
                               (valueType, nonFrozenMap) -> {
                assertEquals(keyType, indexTermType(nonFrozenMap, IndexTarget.Type.KEYS).indexType());
                assertEquals(valueType, indexTermType(nonFrozenMap, IndexTarget.Type.VALUES).indexType());
                IndexTermType entryIndexTermType = indexTermType(nonFrozenMap, IndexTarget.Type.KEYS_AND_VALUES);
                assertEquals(CompositeType.getInstance(keyType, valueType), entryIndexTermType.indexType());
                assertTrue(entryIndexTermType.isComposite());
                assertTrue(entryIndexTermType.isLiteral());
            });
        }
    }

    @Test
    public void testSetType()
    {
        testCollectionType(SetType::getInstance, (a, b) -> {});
    }

    @Test
    public void testListType()
    {
        testCollectionType(ListType::getInstance, (a, b) -> {});
    }

    @Test
    public void testTuple()
    {
        for (CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            TupleType type = TupleType.getInstance(new TypeParser(String.format("(%s, %s)", elementType.getType(), elementType.getType())));
            IndexTermType indexTermType = indexTermType(type, IndexTarget.Type.SIMPLE);
            assertFalse(indexTermType.isFrozenCollection());
            assertTrue(indexTermType.isFrozen());
            assertTrue(indexTermType.isLiteral());
            assertFalse(indexTermType.isReversed());

            IndexTermType reversedIndexTermType = indexTermType(ReversedType.getInstance(type), IndexTarget.Type.SIMPLE);
            assertFalse(reversedIndexTermType.isFrozenCollection());
            assertTrue(reversedIndexTermType.isFrozen());
            assertTrue(reversedIndexTermType.isLiteral());
            assertTrue(reversedIndexTermType.isReversed());
        }
    }

    @Test
    public void testUDT()
    {
        for (CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            UserType type = new UserType("ks", ByteBufferUtil.bytes("myType"),
                                         Arrays.asList(FieldIdentifier.forQuoted("f1"), FieldIdentifier.forQuoted("f2")),
                                         Arrays.asList(elementType.getType(), elementType.getType()),
                                         true);
            IndexTermType indexTermType = indexTermType(type, IndexTarget.Type.SIMPLE);
            assertFalse(indexTermType.isFrozenCollection());
            assertFalse(indexTermType.isFrozen());
            assertFalse(indexTermType.isLiteral());
            assertFalse(indexTermType.isReversed());

            IndexTermType reversedIndexTermType = indexTermType(ReversedType.getInstance(type), IndexTarget.Type.SIMPLE);
            assertFalse(reversedIndexTermType.isFrozenCollection());
            assertFalse(reversedIndexTermType.isFrozen());
            assertFalse(reversedIndexTermType.isLiteral());
            assertTrue(reversedIndexTermType.isReversed());

            type = new UserType("ks", ByteBufferUtil.bytes("myType"),
                                Arrays.asList(FieldIdentifier.forQuoted("f1"), FieldIdentifier.forQuoted("f2")),
                                Arrays.asList(elementType.getType(), elementType.getType()),
                                false);
            indexTermType = indexTermType(type, IndexTarget.Type.SIMPLE);
            assertFalse(indexTermType.isFrozenCollection());
            assertTrue(indexTermType.isFrozen());
            assertTrue(indexTermType.isLiteral());

            reversedIndexTermType = indexTermType(ReversedType.getInstance(type), IndexTarget.Type.SIMPLE);
            assertFalse(reversedIndexTermType.isFrozenCollection());
            assertTrue(reversedIndexTermType.isFrozen());
            assertTrue(reversedIndexTermType.isLiteral());
            assertTrue(reversedIndexTermType.isReversed());
        }
    }

    private static void testCollectionType(BiFunction<AbstractType<?>, Boolean, AbstractType<?>> init,
                                           BiConsumer<AbstractType<?>, AbstractType<?>> nonFrozenCollectionTester)
    {
        for (CQL3Type elementType : StorageAttachedIndex.SUPPORTED_TYPES)
        {
            AbstractType<?> frozenCollection = init.apply(elementType.getType(), false);
            AbstractType<?> reversedFrozenCollection = ReversedType.getInstance(frozenCollection);

            IndexTermType indexTermType = indexTermType(frozenCollection, IndexTarget.Type.FULL);
            assertTrue(indexTermType.isFrozenCollection());
            assertTrue(indexTermType.isLiteral());
            assertFalse(indexTermType.isReversed());

            IndexTermType reversedIndexTermType = indexTermType(reversedFrozenCollection, IndexTarget.Type.FULL);
            assertTrue(reversedIndexTermType.isFrozenCollection());
            assertTrue(reversedIndexTermType.isLiteral());
            assertTrue(reversedIndexTermType.isReversed());

            AbstractType<?> nonFrozenCollection = init.apply(elementType.getType(), true);
            assertEquals(elementType.getType(), indexTermType(nonFrozenCollection, IndexTarget.Type.VALUES).indexType());
            nonFrozenCollectionTester.accept(elementType.getType(), nonFrozenCollection);
        }
    }

    private static IndexTermType indexTermType(AbstractType<?> type, IndexTarget.Type indexType)
    {
        return IndexTermType.create(column(type), Collections.emptyList(), indexType);
    }

    private static ColumnMetadata column(AbstractType<?> type)
    {
        return ColumnMetadata.regularColumn("ks", "cf", "col", type);
    }

    @Test
    public void shouldCompareByteBuffers()
    {
        IndexTermType indexTermType = indexTermType(Int32Type.instance, IndexTarget.Type.SIMPLE);

        final ByteBuffer a = Int32Type.instance.decompose(1);
        final ByteBuffer b = Int32Type.instance.decompose(2);

        assertEquals(a, indexTermType.min(a, b));
        assertEquals(a, indexTermType.min(b, a));
        assertEquals(a, indexTermType.min(a, a));
        assertEquals(b, indexTermType.min(b, b));
        assertEquals(b, indexTermType.min(null, b));
        assertEquals(a, indexTermType.min(a, null));

        assertEquals(b, indexTermType.max(b, a));
        assertEquals(b, indexTermType.max(a, b));
        assertEquals(a, indexTermType.max(a, a));
        assertEquals(b, indexTermType.max(b, b));
        assertEquals(b, indexTermType.max(null, b));
        assertEquals(a, indexTermType.max(a, null));
    }

    @Test
    public void testBigIntegerEncoding()
    {
        BigInteger[] data = new BigInteger[10000];
        for (int i = 0; i < data.length; i++)
        {
            BigInteger randomNumber = getRandom().nextBigInteger(1000);
            if (getRandom().nextBoolean())
                randomNumber = randomNumber.negate();

            data[i] = randomNumber;
        }

        Arrays.sort(data, BigInteger::compareTo);

        IndexTermType indexTermType = indexTermType(IntegerType.instance, IndexTarget.Type.SIMPLE);
        assertTrue(indexTermType.supportsRounding());

        for (int i = 1; i < data.length; i++)
        {
            BigInteger i0 = data[i - 1];
            BigInteger i1 = data[i];
            assertTrue("#" + i, i0.compareTo(i1) <= 0);

            ByteBuffer b0 = indexTermType.asIndexBytes(ByteBuffer.wrap(i0.toByteArray()));
            ByteBuffer b1 = indexTermType.asIndexBytes(ByteBuffer.wrap(i1.toByteArray()));
            assertTrue("#" + i, indexTermType.compare(b0, b1) <= 0);
        }
    }

    @Test
    public void testMapEntryEncoding()
    {
        CompositeType type = CompositeType.getInstance(UTF8Type.instance, Int32Type.instance);
        IndexTermType indexTermType = indexTermType(type, IndexTarget.Type.SIMPLE);

        // simulate: index memtable insertion
        String[] data = new String[10000];
        byte[] temp = new byte[100];
        for (int i = 0; i < data.length; i++)
        {
            getRandom().nextBytes(temp);
            String v1 = new String(temp);
            int v2 = getRandom().nextInt();

            data[i] = indexTermType.asString(type.decompose(v1, v2));
        }

        Arrays.sort(data, String::compareTo);

        for (int i = 1; i < data.length; i++)
        {
            // simulate: index memtable flush
            ByteBuffer b0 = indexTermType.fromString(data[i - 1]);
            ByteBuffer b1 = indexTermType.fromString(data[i]);
            assertTrue("#" + i, indexTermType.compare(b0, b1) <= 0);

            // simulate: saving into on-disk trie
            ByteComparable t0 = ByteComparable.fixedLength(b0);
            ByteComparable t1 = ByteComparable.fixedLength(b1);
            assertTrue("#" + i, ByteComparable.compare(t0, t1, ByteComparable.Version.OSS50) <= 0);
        }
    }
}
