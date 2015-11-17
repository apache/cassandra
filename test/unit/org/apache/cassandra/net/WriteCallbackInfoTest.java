/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.UUID;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.ByteBufferUtil;

public class WriteCallbackInfoTest
{

    @Test
    public void testShouldHint() throws Exception
    {
        testShouldHint(Verb.COUNTER_MUTATION, ConsistencyLevel.ALL, true, false);
        for (Verb verb : new Verb[] { Verb.PAXOS_COMMIT, Verb.MUTATION })
        {
            testShouldHint(verb, ConsistencyLevel.ALL, true, true);
            testShouldHint(verb, ConsistencyLevel.ANY, true, false);
            testShouldHint(verb, ConsistencyLevel.ALL, false, false);
        }
    }

    private void testShouldHint(Verb verb, ConsistencyLevel cl, boolean allowHints, boolean expectHint) throws Exception
    {
        Object payload = verb == Verb.PAXOS_COMMIT
                         ? new Commit(UUID.randomUUID(), new PartitionUpdate(MockSchema.newCFMetaData("", ""), ByteBufferUtil.EMPTY_BYTE_BUFFER, PartitionColumns.NONE, 1))
                         : new Mutation("", new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0), ByteBufferUtil.EMPTY_BYTE_BUFFER));

        WriteCallbackInfo wcbi = new WriteCallbackInfo(InetAddress.getByName("192.168.1.1"), null, new MessageOut(verb, payload, null), null, cl, allowHints);
        Assert.assertEquals(expectHint, wcbi.shouldHint());
        if (expectHint)
        {
            Assert.assertNotNull(wcbi.mutation());
        }
        else
        {
            boolean fail = false;
            try
            {
                wcbi.mutation();
            }
            catch (Throwable t)
            {
                fail = true;
            }
            Assert.assertTrue(fail);
        }
    }
}
