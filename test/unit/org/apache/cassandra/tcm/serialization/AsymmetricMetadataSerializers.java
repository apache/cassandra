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

package org.apache.cassandra.tcm.serialization;

import java.io.IOException;

import org.assertj.core.api.Assertions;

import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class AsymmetricMetadataSerializers
{
    public static <In, Out> void testSerde(DataOutputBuffer output, AsymmetricMetadataSerializer<In, Out> serializer, In input, Version version) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input, version);
        serializer.serialize(input, output, version);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        Out read = serializer.deserialize(in, version);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
    }
}
