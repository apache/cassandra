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

package org.apache.cassandra.cql3;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions.Codec.Flag;

public class QueryOptionsFlagsTest
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
        for (Flag flag : Flag.values())
        {
            flags = Flag.remove(flags, flag);
            Assert.assertFalse(Flag.contains(flags, flag));
            for (int i = flag.ordinal() + 1; i < Flag.values().length; i++)
                Assert.assertTrue(Flag.contains(flags, Flag.values()[i]));
        }

    }

    @Test
    public void checkFlagEncoding()
    {
        int flags = Flag.none();
        flags = Flag.add(flags, Flag.VALUES);
        flags = Flag.add(flags, Flag.PAGING_STATE);
        flags = Flag.add(flags, Flag.TIMESTAMP);

        Assert.assertEquals(flags, 0x0001 | 0x0008 |  0x0020);
    }

    @Test
    public void checkFlagDecoding()
    {
        int flags = 0x0001 | 0x0040 | 0x0004 | 0x0100;
        Assert.assertTrue(Flag.contains(flags, Flag.VALUES));
        Assert.assertTrue(Flag.contains(flags, Flag.NAMES_FOR_VALUES));
        Assert.assertTrue(Flag.contains(flags, Flag.PAGE_SIZE));
        Assert.assertFalse(Flag.contains(flags, Flag.SKIP_METADATA));
        Assert.assertTrue(Flag.contains(flags, Flag.NOW_IN_SECONDS));
    }
}
