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

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.locator.ReplicaUtils.*;

public class ReplicaLayoutTest
{
    @Test
    public void testConflictResolution()
    {
        final Token token = new Murmur3Partitioner.LongToken(1L);
        final Replica f1 = Replica.fullReplica(EP1, R1);
        final Replica f2 = Replica.fullReplica(EP2, R1);
        final Replica t2 = Replica.transientReplica(EP2, R1);
        final Replica f3 = Replica.fullReplica(EP3, R1);
        final Replica t4 = Replica.transientReplica(EP4, R1);

        {
            // test no conflict
            EndpointsForToken natural = EndpointsForToken.of(token, f1, f3);
            EndpointsForToken pending = EndpointsForToken.of(token, t2, t4);
            Assert.assertFalse(ReplicaLayout.haveWriteConflicts(natural, pending));
        }
        {
            // test full in natural, transient in pending
            EndpointsForToken natural = EndpointsForToken.of(token, f1, f2, f3);
            EndpointsForToken pending = EndpointsForToken.of(token, t2, t4);
            EndpointsForToken expectNatural = natural;
            EndpointsForToken expectPending = EndpointsForToken.of(token, t4);
            Assert.assertTrue(ReplicaLayout.haveWriteConflicts(natural, pending));
            assertEquals(expectNatural, ReplicaLayout.resolveWriteConflictsInNatural(natural, pending));
            assertEquals(expectPending, ReplicaLayout.resolveWriteConflictsInPending(natural, pending));
        }
        {
            // test transient in natural, full in pending
            EndpointsForToken natural = EndpointsForToken.of(token, f1, t2, f3);
            EndpointsForToken pending = EndpointsForToken.of(token, f2, t4);
            EndpointsForToken expectNatural = EndpointsForToken.of(token, f1, f2, f3);
            EndpointsForToken expectPending = EndpointsForToken.of(token, t4);
            Assert.assertTrue(ReplicaLayout.haveWriteConflicts(natural, pending));
            assertEquals(expectNatural, ReplicaLayout.resolveWriteConflictsInNatural(natural, pending));
            assertEquals(expectPending, ReplicaLayout.resolveWriteConflictsInPending(natural, pending));
        }
    }

    private static void assertEquals(AbstractReplicaCollection<?> a, AbstractReplicaCollection<?> b)
    {
        Assert.assertEquals(a.list, b.list);
    }

}
