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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.util.function.LongFunction;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;

import static org.junit.Assert.assertEquals;

public class NumericValuesTest extends SAIRandomizedTester
{
    @Test
    public void testMonotonic() throws Exception
    {
        doTest(true);
    }

    @Test
    public void testRegular() throws Exception
    {
        doTest(false);
    }

    @Test
    public void testRepeatsMonotonicValues() throws Exception
    {
        testRepeatedNumericValues(true);
    }

    @Test
    public void testRepeatsRegularValues() throws Exception
    {
        testRepeatedNumericValues(false);
    }

    private void testRepeatedNumericValues(boolean monotonic) throws Exception
    {
        int length = 64_000;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        writeTokens(monotonic, indexDescriptor, new long[length], prev -> 1000L);

        final MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);

        NumericValuesMeta tokensMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.ROW_TO_TOKEN)));

        try (FileHandle fileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.ROW_TO_TOKEN, null);
             LongArray reader = monotonic ? new MonotonicBlockPackedReader(fileHandle, tokensMeta).open()
                                          : new BlockPackedReader(fileHandle, tokensMeta).open())
        {
            for (int x = 0; x < length; x++)
            {
                assertEquals(reader.get(x), 1000);
            }
        }
    }

    private void doTest(boolean monotonic) throws Exception
    {
        final long[] array = new long[64_000];
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        writeTokens(monotonic, indexDescriptor, array, prev -> monotonic ? prev + nextInt(100) : nextLong(0, Long.MAX_VALUE));

        final MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);
        NumericValuesMeta tokensMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.ROW_TO_TOKEN)));

        try (FileHandle fileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.ROW_TO_TOKEN, null);
             LongArray reader = (monotonic ? new MonotonicBlockPackedReader(fileHandle, tokensMeta)
                                           : new BlockPackedReader(fileHandle, tokensMeta)).open())
        {
            assertEquals(array.length, reader.length());

            for (int x = 0; x < array.length; x++)
            {
                assertEquals(array[x], reader.get(x));
            }
        }
    }

    private void writeTokens(boolean monotonic, IndexDescriptor indexDescriptor, long[] array, LongFunction<Long> generator) throws Exception
    {
        final int blockSize = 1 << nextInt(8, 15);

        long current = 0;
        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
             final NumericValuesWriter numericWriter = new NumericValuesWriter(indexDescriptor,
                                                                               IndexComponent.ROW_TO_TOKEN,
                                                                               metadataWriter,
                                                                               monotonic,
                                                                               blockSize))
        {
            for (int x = 0; x < array.length; x++)
            {
                current = generator.apply(current);

                numericWriter.add(current);

                array[x] = current;
            }
        }
    }
}
