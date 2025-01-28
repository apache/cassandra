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

package org.apache.cassandra.transport;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.transport.Envelope.Header.Flag;

public class EnvelopeHeaderFlagsTest
{
    @Test
    public void checkFlagOperations()
    {
        int flags = Flag.none();
        for (Flag flag : Flag.values())
        {
            flags = Flag.add(flags, flag);
            Assert.assertTrue(Flag.contains(flags, flag));
            for (int i = flag.ordinal() + 1; i < Flag.values().length; i++)
                Assert.assertFalse(Flag.contains(flags, Flag.values()[i]));
        }
    }

    @Test
    public void checkFlagEncoding()
    {
        int flags = Flag.none();
        flags = Flag.add(flags, Flag.COMPRESSED);
        flags = Flag.add(flags, Flag.TRACING);
        flags = Flag.add(flags, Flag.USE_BETA);

        Assert.assertEquals(flags, 0x01 | 0x02 | 0x10);
    }

    @Test
    public void checkFlagDecoding()
    {
        int flags = 0x02 | 0x08 | 0x10;
        Assert.assertFalse(Flag.contains(flags, Flag.COMPRESSED));
        Assert.assertTrue(Flag.contains(flags, Flag.TRACING));
        Assert.assertFalse(Flag.contains(flags, Flag.CUSTOM_PAYLOAD));
        Assert.assertTrue(Flag.contains(flags, Flag.WARNING));
        Assert.assertTrue(Flag.contains(flags, Flag.USE_BETA));
    }
}
