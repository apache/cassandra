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

package org.apache.cassandra.replication;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ShortMutationIdTest
{
    private static ShortMutationId id(int hostId, int hostLogId, int offset)
    {
        return new ShortMutationId(CoordinatorLogId.asLong(hostId, hostLogId), offset);
    }

    @Test
    public void testComparison()
    {
        assertTrue(id(1, 0, 0).compareTo(id(2, 0, 0)) < 0);
        assertTrue(id(1, 0, 0).compareTo(id(1, 1, 0)) < 0);
        assertTrue(id(1, 1, 0).compareTo(id(1, 1, 1)) < 0);

        // host id outranks the components below it
        assertTrue(id(2, 0, 0).compareTo(id(1, 9, 9)) > 0);
        assertTrue(id(1, 2, 0).compareTo(id(1, 1, 9)) > 0);
    }

    @Test
    public void compareMatchesEqual()
    {
        MutationId early = new MutationId(CoordinatorLogId.asLong(1, 2), 3, 100);
        MutationId late = new MutationId(CoordinatorLogId.asLong(1, 2), 3, 200);

        ShortMutationId[][] equalPairs = { { id(1, 2, 3), id(1, 2, 3) }, { early, late } };
        for (ShortMutationId[] pair : equalPairs)
        {
            assertEquals(pair[0], pair[1]);
            assertEquals(0, pair[0].compareTo(pair[1]));
            assertEquals(pair[0].hashCode(), pair[1].hashCode());
        }

        for (ShortMutationId other : new ShortMutationId[]{ id(9, 2, 3), id(1, 9, 3), id(1, 2, 9) })
        {
            assertNotEquals(id(1, 2, 3), other);
            assertNotEquals(0, id(1, 2, 3).compareTo(other));
        }
    }

    @Test
    public void convertingFromMutationIdPreservesTheId()
    {
        MutationId full = new MutationId(CoordinatorLogId.asLong(1, 2), 3, 100);
        ShortMutationId shortened = new ShortMutationId(full);

        assertEquals("host id and host log id must not be transposed", full.hostId(), shortened.hostId());
        assertEquals(full.hostLogId(), shortened.hostLogId());
        assertEquals(full.logId(), shortened.logId());
        assertEquals(full.offset(), shortened.offset());
        assertEquals(full, shortened);
    }
}
