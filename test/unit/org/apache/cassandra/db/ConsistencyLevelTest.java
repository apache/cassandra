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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;


public class ConsistencyLevelTest
{
    @Test
    public void testSatisfiesQuorumFor()
    {
        DatabaseDescriptor.daemonInitialization();
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create("test", KeyspaceParams.simple(3)));
        Assert.assertTrue(ConsistencyLevel.ALL.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.EACH_QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.LOCAL_ONE.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.LOCAL_QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.ONE.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.TWO.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.THREE.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.SERIAL.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.LOCAL_SERIAL.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.ANY.satisfiesQuorumFor(keyspace));

        keyspace = Keyspace.mockKS(KeyspaceMetadata.create("test", KeyspaceParams.nts("DC1", 3, "DC2", 3)));
        Assert.assertTrue(ConsistencyLevel.ALL.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.EACH_QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.LOCAL_ONE.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.LOCAL_QUORUM.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.ONE.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.TWO.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.THREE.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.SERIAL.satisfiesQuorumFor(keyspace));
        Assert.assertTrue(ConsistencyLevel.LOCAL_SERIAL.satisfiesQuorumFor(keyspace));
        Assert.assertFalse(ConsistencyLevel.ANY.satisfiesQuorumFor(keyspace));

    }
}
