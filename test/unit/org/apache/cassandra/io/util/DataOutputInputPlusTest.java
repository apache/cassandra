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

package org.apache.cassandra.io.util;

import org.junit.Test;

import accord.utils.Gens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class DataOutputInputPlusTest
{
    @Test
    public void leastSignificantBytes()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(Gens.longs().all()).check(expected -> {
            output.clear();

            int expectedSize = numberOfBytes(expected);
            output.writeLeastSignificantBytes(expected, expectedSize);
            Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
            @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
            long read = in.readLeastSignificantBytes(expectedSize);
            Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input").isEqualTo(expected);
        });
    }

    private static int numberOfBytes(long value)
    {
        return (64 + 7 - Long.numberOfLeadingZeros(value)) / 8;
    }
}
