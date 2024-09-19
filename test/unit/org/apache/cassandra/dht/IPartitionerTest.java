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

package org.apache.cassandra.dht;

import java.util.Objects;

import org.junit.Test;

import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class IPartitionerTest
{
    @Test
    public void byteCompareSerde()
    {
        qt().forAll(AccordGenerators.fromQT(CassandraGenerators.token())).check(token -> {
            var p = token.getPartitioner();
            var comparable = Objects.requireNonNull(ByteSource.peekable(p.getTokenFactory().asComparableBytes(token, ByteComparable.Version.OSS50)));
            Token read = p.getTokenFactory().fromComparableBytes(comparable, ByteComparable.Version.OSS50);
            Assertions.assertThat(read)
                      .describedAs("If LocalPartitioner, the type is %s", (token.getPartitioner() instanceof LocalPartitioner ? AbstractTypeGenerators.typeTree(((LocalPartitioner) token.getPartitioner()).comparator) : null))
                      .isEqualTo(token);
        });
    }
}