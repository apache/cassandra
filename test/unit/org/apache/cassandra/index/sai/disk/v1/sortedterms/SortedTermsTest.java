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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class SortedTermsTest extends AbstractSortedTermsTester
{
    @Test
    public void testFileValidation() throws Exception
    {
        List<PrimaryKey> primaryKeys = new ArrayList<>();

        for (int x = 0; x < 11; x++)
        {
            ByteBuffer buffer = UTF8Type.instance.decompose(Integer.toString(x));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(buffer);
            PrimaryKey primaryKey = SAITester.TEST_FACTORY.create(partitionKey, Clustering.EMPTY);
            primaryKeys.add(primaryKey);
        }

        primaryKeys.sort(PrimaryKey::compareTo);

        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META)))
        {
            IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (SortedTermsWriter writer = new SortedTermsWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS),
                                                                  metadataWriter,
                                                                  bytesWriter,
                                                                  blockFPWriter,
                                                                  trieWriter))
            {
                primaryKeys.forEach(primaryKey -> {
                    try
                    {
                        writer.add(primaryKey::asComparableBytes);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_TRIE, true));
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_TRIE, false));
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_BLOCKS, true));
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_BLOCKS, false));
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS, true));
        assertTrue(validateComponent(indexDescriptor, IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS, false));
    }

    @Test
    public void testLongPrefixesAndSuffixes() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(writer ->
        {
            // The following writes a lexographically ordered set of terms that cover the following
            // conditions:

            // Start value 0
            byte[] bytes = new byte[20];
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix > 15
            bytes = new byte[20];
            Arrays.fill(bytes, 16, 20, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix == 15
            bytes = new byte[20];
            Arrays.fill(bytes, 15, 20, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix < 15
            bytes = new byte[20];
            Arrays.fill(bytes, 14, 20, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix > 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 4, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix == 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 5, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix < 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 6, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));

            bytes = new byte[32];
            Arrays.fill(bytes, 0, 16, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix >= 15 && suffix >= 16
            bytes = new byte[32];
            Arrays.fill(bytes, 0, 32, (byte)1);
            terms.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
        });

        doTestSortedTerms(terms);
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();

        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        });

        doTestSortedTerms(terms);
    }

    private void doTestSortedTerms(List<byte[]> terms) throws Exception
    {
        // iterate ascending
        withSortedTermsCursor(cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS50));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate descending
        withSortedTermsCursor(cursor ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS50));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate randomly
        withSortedTermsCursor(cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());
                cursor.seekToPointId(target);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS50));
                assertArrayEquals(terms.get(target), bytes);
            }
        });
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        writeTerms(writer ->
        {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        });

        withSortedTermsCursor(cursor ->
        {
            assertThatThrownBy(() -> cursor.seekToPointId(-2)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> cursor.seekToPointId(Long.MAX_VALUE)).isInstanceOf(IndexOutOfBoundsException.class);
        });
    }

    private boolean validateComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, boolean checksum)
    {
        try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
            return true;
        }
        catch (Throwable ignore)
        {
            return false;
        }
    }
}
