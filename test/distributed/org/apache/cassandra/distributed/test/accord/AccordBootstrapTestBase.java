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

package org.apache.cassandra.distributed.test.accord;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;

import accord.api.ConfigurationService.EpochReady;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableFunction;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordConfigurationService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.service.accord.AccordConfigurationService.EpochSnapshot.ResultStatus.SUCCESS;
import static org.apache.cassandra.service.accord.AccordConfigurationService.SyncStatus.COMPLETED;

public class AccordBootstrapTestBase extends TestBaseImpl
{
    static DecoratedKey dk(int key)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        return partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    static PartitionKey pk(int key, String keyspace, String table)
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        return new PartitionKey(tid, dk(key));
    }

    protected IInvokableInstance bootstrapAndJoinNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        config.set("accord.shard_durability_target_splits", "1");
        config.set("accord.shard_durability_cycle", "20s");
        IInvokableInstance newInstance = cluster.bootstrap(config);
        newInstance.startup(cluster);
        // todo: re-add once we fix write survey/join ring = false mode
//        withProperty(BOOTSTRAP_SCHEMA_DELAY_MS.getKey(), Integer.toString(90 * 1000),
//                     () -> withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster)));
//        newInstance.nodetoolResult("join").asserts().success();
        newInstance.nodetoolResult("cms", "describe").asserts().success(); // just make sure we're joined, remove later
        return newInstance;
    }

    static AccordService service()
    {
        return (AccordService) AccordService.instance();
    }

    static void awaitEpoch(long epoch, Function<EpochReady, AsyncResult<Void>> await)
    {
        try
        {
            boolean completed = service().epochReady(Epoch.create(epoch), await).await(60, TimeUnit.SECONDS);
            Assertions.assertThat(completed)
                      .describedAs("Epoch %s did not become ready within timeout on %s -> %s",
                                   epoch, FBUtilities.getBroadcastAddressAndPort(),
                                   service().configService().getEpochSnapshot(epoch))
                      .isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    static void awaitLocalSyncNotification(long epoch)
    {
        try
        {
            AccordConfigurationService configService = service().configService();
            boolean completed = configService.unsafeLocalSyncNotified(epoch).await(60, TimeUnit.SECONDS);
            Assert.assertTrue(String.format("Local sync notification for epoch %s did not become ready within timeout on %s\n%s",
                                            epoch, FBUtilities.getBroadcastAddressAndPort(), service().configService().getDebugStr()), completed);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    static long maxEpoch(Cluster cluster)
    {
        return cluster.stream().mapToLong(node -> {
            if (node.isShutdown())
                return 0L;
            return node.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
        }).max().getAsLong();
    }

    static long awaitMaxEpochReadyToRead(Cluster cluster)
    {
        return awaitMaxEpoch(cluster, EpochReady::reads, true);
    }

    static long awaitMaxEpochMetadataReady(Cluster cluster)
    {
        return awaitMaxEpoch(cluster, EpochReady::metadata, false);
    }

    static long awaitMaxEpoch(Cluster cluster, SerializableFunction<EpochReady, AsyncResult<Void>> await, boolean expectReadyToRead)
    {
        long maxEpoch = maxEpoch(cluster);
        for (IInvokableInstance node : cluster)
        {
            if (node.isShutdown())
                continue;

            node.acceptOnInstance(aw -> {
                ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(maxEpoch));
                Assert.assertEquals(maxEpoch, ClusterMetadata.current().epoch.getEpoch());
                AccordService service = (AccordService) AccordService.instance();
                awaitEpoch(maxEpoch, aw);
                AccordConfigurationService configService = service.configService();

                awaitLocalSyncNotification(maxEpoch);
                for (long epoch = configService.minEpoch(); epoch <= maxEpoch; epoch++)
                {
                    Assert.assertEquals(COMPLETED, configService.getEpochSnapshot(maxEpoch).syncStatus);
                    Assert.assertEquals(SUCCESS, configService.getEpochSnapshot(maxEpoch).acknowledged);
                    Assert.assertEquals(SUCCESS, configService.getEpochSnapshot(maxEpoch).received);
                    if (expectReadyToRead)
                        Assert.assertEquals(SUCCESS, configService.getEpochSnapshot(maxEpoch).reads);
                }
            }, node.transfer(await));
        }
        return maxEpoch;
    }
}
