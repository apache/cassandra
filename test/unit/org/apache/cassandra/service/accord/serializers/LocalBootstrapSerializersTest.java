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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Test;

import accord.local.Bootstrap;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.utils.Property.qt;

public class LocalBootstrapSerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Gen<TxnId> txnIdGen = AccordGens.txnIds();
    private static final Gen<Ranges> rangesGen = AccordGenerators.ranges();
    private static final MessagingService.Version minVersion = MessagingService.Version.VERSION_51;
    private static final Iterable<MessagingService.Version> versions = versions();

    @Test
    public void markBootstrapComplete()
    {
        qt().forAll(markBootstrapCompleteGen()).check(e -> {
            maybeUpdatePartitioner(e.ranges);
            for (MessagingService.Version v : versions)
                serde(LocalBootstrapSerializers.markBootstrapComplete, e, v.value);
        });
    }

    @Test
    public void createBootstrapCompleteMarkerTransaction()
    {
        qt().forAll(createBootstrapCompleteMarkerTransactionGen()).check(e -> {
            maybeUpdatePartitioner(e.valid);
            for (MessagingService.Version v : versions)
                serde(LocalBootstrapSerializers.createBootstrapCompleteMarkerTransaction, e, v.value);
        });
    }

    private static Iterable<MessagingService.Version> versions()
    {
        MessagingService.Version[] array = MessagingService.Version.values();
        return Arrays.stream(array, minVersion.ordinal(), array.length)
                     .collect(Collectors.toList());
    }

    private static IPartitioner getPartitioner(Ranges ranges)
    {
        IPartitioner partitioner = null;
        for (Range r : ranges)
        {
            AccordRoutingKey start = (AccordRoutingKey) r.start();
            if (start.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
            {
                AccordRoutingKey end = (AccordRoutingKey) r.end();
                if (end.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
                {
                    // can't figure out partition...
                    continue;
                }
                partitioner = end.token().getPartitioner();
            }
            else
            {
                partitioner = start.token().getPartitioner();
            }
            break;
        }
        return partitioner;
    }

    private static void maybeUpdatePartitioner(Ranges ranges)
    {
        // try to set partitioner
        IPartitioner partitioner = getPartitioner(ranges);
        if (partitioner != null)
        {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Schema.instance = Mockito.mock(SchemaProvider.class);
            try
            {
                Mockito.when(Schema.instance.getExistingTablePartitioner(Mockito.any())).thenReturn(partitioner);
            }
            catch (UnknownTableException e)
            {
                throw new AssertionError();
            }
        }
    }

    private static <T> void serde(IVersionedSerializer<T> serializer, T e, int version) throws IOException
    {
        long sizeOf = serializer.serializedSize(e, version);

        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            serializer.serialize(e, out, version);
            ByteBuffer bb = out.unsafeGetBufferAndFlip();
            Assertions.assertThat(bb.remaining()).isEqualTo(sizeOf);

            DataInputBuffer in = new DataInputBuffer(bb, false);
            T read = serializer.deserialize(in, version);
            Assertions.assertThat(read).isEqualTo(e);
        }
    }


    private static Gen<Bootstrap.MarkBootstrapComplete> markBootstrapCompleteGen()
    {
        return rs -> {
            int storeId = rs.nextInt(0, Integer.MAX_VALUE);
            TxnId txnId = txnIdGen.next(rs);
            Ranges ranges = rangesGen.next(rs);
            return new Bootstrap.MarkBootstrapComplete(storeId, txnId, ranges);
        };
    }

    private static Gen<Bootstrap.CreateBootstrapCompleteMarkerTransaction> createBootstrapCompleteMarkerTransactionGen()
    {
        return rs -> {
            int storeId = rs.nextInt(0, Integer.MAX_VALUE);
            TxnId txnId = txnIdGen.next(rs);
            Ranges valid = rangesGen.filter(r -> !r.isEmpty()).next(rs);
            IPartitioner partitioner = getPartitioner(valid);

            SyncPoint<Ranges> syncPoint = new SyncPoint<>(txnIdGen.next(rs), AccordGenerators.depsGen(partitioner).next(rs), valid, valid.toRoute(valid.get(0).end()));
            return new Bootstrap.CreateBootstrapCompleteMarkerTransaction(storeId, txnId, syncPoint, valid);
        };
    }
}