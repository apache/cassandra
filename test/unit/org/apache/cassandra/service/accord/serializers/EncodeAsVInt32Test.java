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

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static accord.utils.Property.qt;

public class EncodeAsVInt32Test
{
    private static final Gen.IntGen ENUM_RANGE = Gens.ints().between(0, Integer.MAX_VALUE - 1);

    @Test
    public void withNulls()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        EncodeAsVInt32<Integer> serializer = EncodeAsVInt32.withNulls(Integer::intValue, Integer::valueOf);
        qt().forAll(ENUM_RANGE).check(expected -> Serializers.testSerde(output, serializer, expected));
    }

    @Test
    public void withoutNulls()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        EncodeAsVInt32<Integer> serializer = EncodeAsVInt32.withoutNulls(Integer::intValue, Integer::valueOf);
        qt().forAll(Gens.ints().all()).check(expected -> Serializers.testSerde(output, serializer, expected));
    }
}