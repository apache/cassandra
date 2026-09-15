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

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * A reusable key's bytes and token move with every {@code copyKey}, so {@code retainable()} must
 * hand back a key that stays put after the next copy.
 */
public class ReusableDecoratedKeyTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void retainableSurvivesTheNextKeyOnMurmur3()
    {
        assertRetainableSurvivesTheNextKey(Murmur3Partitioner.instance);
    }

    @Test
    public void retainableSurvivesTheNextKeyOnLocalPartitioner()
    {
        assertRetainableSurvivesTheNextKey(new LocalPartitioner(Int32Type.instance));
    }

    private static void assertRetainableSurvivesTheNextKey(IPartitioner partitioner)
    {
        byte[] first = ByteBufferUtil.bytes(1).array();
        byte[] second = ByteBufferUtil.bytes(2).array();
        DecoratedKey expectedFirst = partitioner.decorateKey(ByteBuffer.wrap(first));
        DecoratedKey expectedSecond = partitioner.decorateKey(ByteBuffer.wrap(second));

        ReusableDecoratedKey reusable = partitioner.createReusableKey(0);
        reusable.copyKey(first, first.length);
        assertEquals(expectedFirst, reusable);
        // By order, not equals: the reusable key's token is a subclass, and LongToken.equals is
        // class-strict. The detached copy below is checked with equals.
        assertEquals(0, expectedFirst.getToken().compareTo(reusable.getToken()));

        DecoratedKey retained = reusable.retainable();
        assertNotSame(reusable, retained);
        reusable.copyKey(second, second.length);

        assertEquals(expectedFirst, retained);
        assertEquals(expectedFirst.getToken(), retained.getToken());
        assertEquals(expectedFirst.getKey(), retained.getKey());
        assertEquals(expectedSecond, reusable);
        // By order for the same reason as above
        assertEquals(0, expectedSecond.getToken().compareTo(reusable.getToken()));
    }
}
