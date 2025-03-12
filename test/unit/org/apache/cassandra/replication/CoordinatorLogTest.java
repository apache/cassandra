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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;

public class CoordinatorLogTest
{
    private static final int LOCAL_HOST_ID = 1;
    private static final CoordinatorLogId LOG_ID = new CoordinatorLogId(LOCAL_HOST_ID, 1);
    private static final Participants PARTICIPANTS = new Participants(List.of(LOCAL_HOST_ID, 2, 3));

    private static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static Offsets toOffsets(MutationId... ids)
    {
        Offsets list = new Offsets();
        for (MutationId id : ids)
            list.append(id.offset());
        return list;
    }

    private static void assertUnreconciled(Token token, CoordinatorLog log, Offsets expectedReconciled, MutationId... expectedIds)
    {
        Offsets reconciled = new Offsets();
        Offsets unreconciled = new Offsets();
        log.lookUpUnreconciled(token, unreconciled, reconciled);

        for (MutationId mid : expectedIds)
            Assert.assertTrue(unreconciled.contains(mid.offset()));

        Assert.assertEquals(toOffsets(expectedIds), unreconciled);
        Assert.assertEquals(expectedReconciled, reconciled);
    }

    @Test
    public void remoteReconciliationTest()
    {
        Token tk = tk(1);
        CoordinatorLogPrimary log = new CoordinatorLogPrimary(LOCAL_HOST_ID, LOG_ID, PARTICIPANTS);
        MutationId[] ids = new MutationId[] {
                log.nextId(),
                log.nextId(),
                log.nextId(),
        };

        for (MutationId id : ids)
            log.witnessedMutationLocal(id, tk);

        Offsets reconciled = new Offsets();
        assertUnreconciled(tk, log, reconciled, ids);

        log.witnessedMutationRemote(ids[0], PARTICIPANTS.get(1));
        assertUnreconciled(tk, log, reconciled, ids);

        log.witnessedMutationRemote(ids[0], PARTICIPANTS.get(2));
        reconciled.add(ids[0].offset());
        assertUnreconciled(tk, log, reconciled, ids[1], ids[2]);
    }
}
