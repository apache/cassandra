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

package org.apache.cassandra.gms;

import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;

public class VersionedValueTest
{
    @Test
    public void serde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(values()).check(state -> {
            for (MessagingService.Version version : MessagingService.Version.supportedVersions())
                IVersionedSerializers.testSerde(buffer, VersionedValue.serializer, state, version.value);
        });
    }

    private static Gen<VersionedValue> values()
    {
        return Generators.toGen(CassandraGenerators.partitioners().flatMap(CassandraGenerators::gossipStatusValue))
                         // when status == "" this returns null... skip
                         .filter(vv -> vv != null)
                         // sometimes the text is too big, must not be larger than Short.MAX_VALUE
                         .filter(vv -> TypeSizes.encodedUTF8Length(vv.value) <= Short.MAX_VALUE);
    }
}
