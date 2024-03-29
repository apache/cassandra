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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.serializers.AccordRoutingKeyByteSource.FixedLength;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.CassandraGenerators.token;

public class AccordRoutingKeyByteSourceTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        // AccordRoutingKey$TokenKey reaches into DD to get partitioner, so need to set that up...
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void tokenSerde()
    {
        qt().forAll(fromQT(token())).check(token -> {
            var serializer = AccordRoutingKeyByteSource.create(token.getPartitioner());
            byte[] min = ByteSourceInverse.readBytes(serializer.minAsComparableBytes());
            byte[] max = ByteSourceInverse.readBytes(serializer.maxAsComparableBytes());

            var bytes = serializer.serialize(token);
            if (serializer instanceof FixedLength)
            {
                FixedLength fl = (FixedLength) serializer;
                Assertions.assertThat(bytes)
                          .hasSize(fl.valueSize())
                          .hasSize(min.length)
                          .hasSize(max.length);
            }

            Assertions.assertThat(ByteArrayUtil.compareUnsigned(min, 0, bytes, 0, bytes.length)).isLessThan(0);
            Assertions.assertThat(ByteArrayUtil.compareUnsigned(max, 0, bytes, 0, bytes.length)).isGreaterThan(0);

            var read = serializer.tokenFromComparableBytes(ByteArrayAccessor.instance, bytes);
            Assertions.assertThat(read).isEqualTo(token);
        });
    }

    @Test
    public void accordRoutingKeySerde()
    {
        qt().forAll(AccordGenerators.routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(token()))).check(key -> {
            AccordRoutingKeyByteSource.Serializer serializer = key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL ?
                                                               // doesn't really matter...
                                                               new AccordRoutingKeyByteSource.VariableLength(ByteOrderedPartitioner.instance, ByteComparable.Version.OSS50)
                                                                                                                                  : AccordRoutingKeyByteSource.create(key.asTokenKey().token().getPartitioner());

            var read = serializer.fromComparableBytes(ByteArrayAccessor.instance, serializer.serialize(key));
            Assertions.assertThat(read).isEqualTo(key);
        });
    }
}