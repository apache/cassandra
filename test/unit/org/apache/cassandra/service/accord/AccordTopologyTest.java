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

package org.apache.cassandra.service.accord;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.topology.Topology;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class AccordTopologyTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        System.setProperty(Config.PROPERTY_PREFIX + "partitioner", Murmur3Partitioner.class.getSimpleName());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Test
    public void minMaxTokenTest()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Topology topology = AccordTopologyUtils.createTopology(1);
        Assert.assertNotEquals(0, topology.size());
        TableId tableId = Schema.instance.getTableMetadata("ks", "tbl").id;
        Token minToken = partitioner.getMinimumToken();
        Token maxToken = partitioner.getMaximumToken();

//        topology.forKey(new AccordKey.TokenKey(tableId, minToken.minKeyBound()));
        topology.forKey(new AccordKey.PartitionKey(tableId, new BufferDecoratedKey(minToken, ByteBufferUtil.bytes(0))));
//        topology.forKey(new AccordKey.TokenKey(tableId, minToken.maxKeyBound()));
//        topology.forKey(new AccordKey.TokenKey(tableId, maxToken.minKeyBound()));
        topology.forKey(new AccordKey.PartitionKey(tableId, new BufferDecoratedKey(maxToken, ByteBufferUtil.bytes(0))));
//        topology.forKey(new AccordKey.TokenKey(tableId, maxToken.maxKeyBound()));
    }
}
