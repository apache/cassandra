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
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;

public class ReplicationFactorTest
{
    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        Gossiper.instance.start(1);
    }

    @Test
    public void shouldParseValidRF()
    {
        assertRfParse("0", 0, 0);
        assertRfParse("3", 3, 0);
        assertRfParse("3/1", 3, 1);
        assertRfParse("5", 5, 0);
        assertRfParse("5/2", 5, 2);
    }

    @Test
    public void shouldFailOnInvalidRF()
    {
        assertRfParseFailure("-1", "Replication factor must be non-negative");
        assertRfParseFailure("3/3", "Transient replicas must be zero, or less than total replication factor");
        assertRfParseFailure("3/-1", "Amount of transient nodes should be strictly positive");
        assertRfParseFailure("3/4", "Transient replicas must be zero, or less than total replication factor");
        assertRfParseFailure("3/", "Replication factor format is <replicas> or <replicas>/<transient>");
        assertRfParseFailure("1/a", "For input string");
        assertRfParseFailure("a/1", "For input string");
        assertRfParseFailure("", "For input string");
    }

    @Test
    public void shouldRoundTripParseSimpleRF()
    {
        String rf = "3";
        assertEquals(rf, ReplicationFactor.fromString(rf).toParseableString());
    }

    @Test
    public void shouldRoundTripParseTransientRF()
    {
        String rf = "3/1";
        assertEquals(rf, ReplicationFactor.fromString(rf).toParseableString());
    }

    private static void assertRfParseFailure(String s, String error)
    {
        try
        {
            ReplicationFactor.fromString(s);
            Assert.fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            Assertions.assertThat(e.getMessage()).contains(error);
        }
    }

    private static void assertRfParse(String s, int expectedReplicas, int expectedTrans)
    {
        ReplicationFactor rf = ReplicationFactor.fromString(s);
        assertEquals(expectedReplicas, rf.allReplicas);
        assertEquals(expectedTrans, rf.transientReplicas());
        assertEquals(expectedReplicas - expectedTrans, rf.fullReplicas);
    }
}
