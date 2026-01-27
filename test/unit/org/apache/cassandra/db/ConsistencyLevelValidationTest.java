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

package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.locator.EndpointsForToken;
import org.junit.Test;

import static org.apache.cassandra.db.ConsistencyLevel.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConsistencyLevelValidationTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void expectThrows(Class<? extends Throwable> expected, Runnable r)
    {
        try
        {
            r.run();
            fail("Expected " + expected.getSimpleName() + " to be thrown");
        }
        catch (Throwable t)
        {
            if (!expected.isInstance(t))
                fail("Expected " + expected.getSimpleName() + " but was " + t);
        }
    }

    private static Keyspace ksWithNTS()
    {
        java.util.Map<String, String> replication = new java.util.HashMap<>();
        replication.put("class", "NetworkTopologyStrategy");
        replication.put("DC1", "3");
        return Keyspace.mockKS(KeyspaceMetadata.create("ks_nts", KeyspaceParams.create(false, replication)));
    }

    private Keyspace ksWithSimple(int rf)
    {
        return Keyspace.mockKS(KeyspaceMetadata.create("ks_simple", KeyspaceParams.simple(rf)));
    }

    private static TableMetadata counterTable()
    {
        return TableMetadata.builder("ks", "ctr_tbl")
                            .addPartitionKeyColumn("pk", AsciiType.instance)
                            .addRegularColumn("c", CounterColumnType.instance)
                            .build();
    }

    @Test
    public void validateForCasCommitAllowsCommonWriteCLs()
    {
        AbstractReplicationStrategy rs = ksWithNTS().getReplicationStrategy();
        QUORUM.validateForCasCommit(rs);
        ALL.validateForCasCommit(rs);
        ONE.validateForCasCommit(rs);
        TWO.validateForCasCommit(rs);
        THREE.validateForCasCommit(rs);
        LOCAL_ONE.validateForCasCommit(rs);
        LOCAL_QUORUM.validateForCasCommit(rs);
        ANY.validateForCasCommit(rs);
    }

    @Test
    public void validateForCasCommitRejectsSerialAndRemote()
    {
        AbstractReplicationStrategy rs = ksWithNTS().getReplicationStrategy();
        expectThrows(InvalidRequestException.class, () -> SERIAL.validateForCasCommit(rs));
        expectThrows(InvalidRequestException.class, () -> LOCAL_SERIAL.validateForCasCommit(rs));
        expectThrows(InvalidRequestException.class, () -> REMOTE_QUORUM.validateForCasCommit(rs));
    }

    @Test
    public void validateForCasCommitEachQuorumRequiresNTS()
    {
        // With NTS: ok
        ksWithNTS().getReplicationStrategy(); // ensure NTS constructed
        EACH_QUORUM.validateForCasCommit(ksWithNTS().getReplicationStrategy());

        // With SimpleStrategy: invalid
        AbstractReplicationStrategy simple = ksWithSimple(3).getReplicationStrategy();
        expectThrows(InvalidRequestException.class, () -> EACH_QUORUM.validateForCasCommit(simple));
    }

    @Test
    public void validateCounterForWriteRejectsAnyAndRemoteAndSerial()
    {
        TableMetadata tm = counterTable();
        expectThrows(InvalidRequestException.class, () -> ANY.validateCounterForWrite(tm));
        expectThrows(InvalidRequestException.class, () -> REMOTE_QUORUM.validateCounterForWrite(tm));
        expectThrows(InvalidRequestException.class, () -> SERIAL.validateCounterForWrite(tm));
        expectThrows(InvalidRequestException.class, () -> LOCAL_SERIAL.validateCounterForWrite(tm));
    }

    @Test
    public void validateCounterForWriteAllowsCommonWriteCLs()
    {
        TableMetadata tm = counterTable();
        QUORUM.validateCounterForWrite(tm);
        ALL.validateCounterForWrite(tm);
        ONE.validateCounterForWrite(tm);
        TWO.validateCounterForWrite(tm);
        THREE.validateCounterForWrite(tm);
        LOCAL_ONE.validateCounterForWrite(tm);
        LOCAL_QUORUM.validateCounterForWrite(tm);
    }

    @Test
    public void validateForCasAcceptsSerialLevels()
    {
        SERIAL.validateForCas();
        LOCAL_SERIAL.validateForCas();
    }

    @Test
    public void validateForCasRejectsNonSerialLevels()
    {
        expectThrows(InvalidRequestException.class, () -> ANY.validateForCas());
        expectThrows(InvalidRequestException.class, () -> ONE.validateForCas());
        expectThrows(InvalidRequestException.class, () -> TWO.validateForCas());
        expectThrows(InvalidRequestException.class, () -> THREE.validateForCas());
        expectThrows(InvalidRequestException.class, () -> QUORUM.validateForCas());
        expectThrows(InvalidRequestException.class, () -> ALL.validateForCas());
        expectThrows(InvalidRequestException.class, () -> LOCAL_ONE.validateForCas());
        expectThrows(InvalidRequestException.class, () -> LOCAL_QUORUM.validateForCas());
        expectThrows(InvalidRequestException.class, () -> EACH_QUORUM.validateForCas());
        expectThrows(InvalidRequestException.class, () -> REMOTE_QUORUM.validateForCas());
    }

    @Test
    public void blockForRemoteQuorumWithSimpleStrategy()
    {
        AbstractReplicationStrategy rs = ksWithSimple(3).getReplicationStrategy();
        assertEquals(LOCAL_QUORUM.blockFor(rs), REMOTE_QUORUM.blockFor(rs));
    }

    @Test
    public void blockForWriteRemoteQuorumWithSimpleStrategy()
    {
        AbstractReplicationStrategy rs = ksWithSimple(3).getReplicationStrategy();
        EndpointsForToken pending = EndpointsForToken.empty(DatabaseDescriptor.getPartitioner().getMinimumToken());
        assertEquals(LOCAL_QUORUM.blockForWrite(rs, pending), REMOTE_QUORUM.blockForWrite(rs, pending));
    }
}
