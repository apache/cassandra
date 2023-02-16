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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.AccordJournal.Key;
import org.apache.cassandra.utils.AsymmetricOrdering;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FBUtilities.Order;
import org.checkerframework.checker.nullness.qual.Nullable;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class AccordJournalTest
{
    @Test
    public void keySerde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(keyGen()).check(key ->
        {
            buffer.clear();
            int expectedSize = Key.SUPPORT.serializedSize(1);
            Key.SUPPORT.serialize(key, buffer, 1);
            assertThat(buffer.getLength()).isEqualTo(expectedSize);
            try (DataInputBuffer input = new DataInputBuffer(buffer.unsafeGetBufferAndFlip(), false))
            {
                Key read = Key.SUPPORT.deserialize(input, 1);
                assertThat(read).isEqualTo(key);
            }
        });
    }

    @Test
    public void compareKeys()
    {
        qt().forAll(Gens.lists(keyGen()).ofSizeBetween(2, 100)).check(keys ->
        {
            keys.sort(Key.SUPPORT);

            List<ByteBuffer> buffers = new ArrayList<>(keys.size());
            for (Key k : keys) buffers.add(toBuffer(k));

            for (int i = 0; i < keys.size(); i++)
            {
                Key outerKey = keys.get(i);
                for (int j = 0; j < keys.size(); j++)
                {
                    Key innerKey = keys.get(j);
                    ByteBuffer innerBuffer = buffers.get(j);
                    Order expected = FBUtilities.compare(outerKey, innerKey, Key.SUPPORT);
                    Order actual = FBUtilities.compare(outerKey, innerBuffer, new AsymmetricOrdering<Key, ByteBuffer>()
                    {
                        @Override
                        public int compareAsymmetric(Key left, ByteBuffer right)
                        {
                            return Key.SUPPORT.compareWithKeyAt(left, right, 0, 1);
                        }

                        @Override
                        public int compare(@Nullable Key left, @Nullable Key right)
                        {
                            throw new UnsupportedOperationException();
                        }
                    });
                    assertThat(actual).isEqualTo(expected);
                }
            }
        });
    }

    private static ByteBuffer toBuffer(Key k)
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer(Key.SUPPORT.serializedSize(1)))
        {
            Key.SUPPORT.serialize(k, buffer, 1);
            return buffer.unsafeGetBufferAndFlip();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private Gen<Key> keyGen()
    {
        Gen<TxnId> txnIdGen = AccordGens.txnIds();
        Gen<AccordJournal.Type> typeGen = Gens.enums().all(AccordJournal.Type.class);
        return rs -> new Key(txnIdGen.next(rs), typeGen.next(rs));
    }
}
