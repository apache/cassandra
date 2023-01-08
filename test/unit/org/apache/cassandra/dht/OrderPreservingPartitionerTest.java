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

import java.io.IOException;
import java.math.BigInteger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

public class OrderPreservingPartitionerTest extends PartitionerTestCase
{
    @BeforeClass
    public static void cleanStatesFromPreviousTest() throws IOException
    {
        // Since OrderPreservingPartitioner#describeOwnership tries to read SSTables,
        // we need to clear data dir to clear garbage from previous test before running tests.
        SchemaLoader.cleanupAndLeaveDirs();
    }

    public void initPartitioner()
    {
        partitioner = OrderPreservingPartitioner.instance;
    }

    protected boolean shouldStopRecursion(Token left, Token right)
    {
        return false;
    }

    @Override
    protected void checkRoundTrip(Token original, Token roundTrip)
    {
        StringToken orig = (StringToken) original;
        StringToken rt = (StringToken) roundTrip;
        Assert.assertEquals(orig.token, rt.token.substring(0, orig.token.length()));
        Assert.assertTrue(rt.token.substring(orig.token.length()).matches("\0*"));
    }
}
