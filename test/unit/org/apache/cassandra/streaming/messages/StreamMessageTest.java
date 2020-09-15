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

package org.apache.cassandra.streaming.messages;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.fail;

public class StreamMessageTest
{
    @Test
    public void testTypeLookup() // CASSANDRA-15965
    {
        List<Integer> ids = Arrays.stream(StreamMessage.Type.values())
                                  .mapToInt(t -> t.id)
                                  .boxed()
                                  .collect(Collectors.toList());

        for (int i: ids)
        {
            assertThat(StreamMessage.Type.lookupById(i), instanceOf(StreamMessage.Type.class));
        }

        int max = Collections.max(ids);
        int min = Collections.min(ids);
        int[] badTypes = {0,
                          -1,
                          min - 1, // right now this is redundant to zero
                          max + 1,
                          Integer.MAX_VALUE,
                          Integer.MIN_VALUE};

        for (int t: badTypes)
        {
            try
            {
                StreamMessage.Type.lookupById(t);
                fail("IllegalArgumentException was not thrown for " + t);
            }
            catch(IllegalArgumentException iae)
            {
            }
        }

    }
}
