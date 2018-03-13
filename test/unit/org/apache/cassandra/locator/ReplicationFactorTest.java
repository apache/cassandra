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

package org.apache.cassandra.locator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;

public class ReplicationFactorTest
{

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        Gossiper.instance.start(1);
    }

    private static void assertRfParseFailure(String s)
    {
        try
        {
            ReplicationFactor.fromString(s);
            Assert.fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private static void assertRfParse(String s, int expectedReplicas, int expectedTrans)
    {
        ReplicationFactor rf = ReplicationFactor.fromString(s);
        Assert.assertEquals(expectedReplicas, rf.allReplicas);
        Assert.assertEquals(expectedTrans, rf.transientReplicas());
        Assert.assertEquals(expectedReplicas - expectedTrans, rf.fullReplicas);
    }

    @Test
    public void parseTest()
    {
        assertRfParse("3", 3, 0);
        assertRfParse("3/1", 3, 1);

        assertRfParse("5", 5, 0);
        assertRfParse("5/2", 5, 2);

        assertRfParseFailure("-1");
        assertRfParseFailure("3/3");
        assertRfParseFailure("3/4");
    }

    @Test
    public void roundTripParseTest()
    {
        String input = "3";
        Assert.assertEquals(input, ReplicationFactor.fromString(input).toParseableString());

        String transientInput = "3/1";
        Assert.assertEquals(transientInput, ReplicationFactor.fromString(transientInput).toParseableString());
    }
}
