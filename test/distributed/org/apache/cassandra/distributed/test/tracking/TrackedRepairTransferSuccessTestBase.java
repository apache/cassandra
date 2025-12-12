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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

public abstract class TrackedRepairTransferSuccessTestBase extends TrackedRepairTransferTestBase
{
    protected static Cluster cluster;

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testFullRepairShardAlignedSinglePlan() throws IOException
    {
        String keyspace = "full_repair_shard_aligned_single_plan";
        testFullRepairSinglePlan(keyspace, cluster, KEY_200, 2, false, "repair", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsSinglePlan() throws IOException
    {
        String keyspace = "full_repair_single_plan";
        testFullRepairSinglePlan(keyspace, cluster, KEY_200, 2, false, "repair", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedSinglePlanOptimized() throws IOException
    {
        String keyspace = "full_repair_shard_aligned_single_plan_optimized";
        testFullRepairSinglePlan(keyspace, cluster, KEY_200, 3, true, "repair", "--optimise-streams", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsSinglePlanOptimized() throws IOException
    {
        String keyspace = "full_repair_single_plan_optimized";
        testFullRepairSinglePlan(keyspace, cluster, KEY_200, 3, true, "repair", "--optimise-streams", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedRemoteSender() throws IOException
    {
        String keyspace = "full_repair_shard_aligned_remote_sender";
        testFullRepairRemoteSender(keyspace, cluster, 2, false, "repair", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsRemoteSender() throws IOException
    {
        String keyspace = "full_repair_remote_sender";
        testFullRepairRemoteSender(keyspace, cluster, 2, false, "repair", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedRemoteSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_aligned_optimized_remote_sender";
        testFullRepairRemoteSender(keyspace, cluster, 3, true, "repair", "--optimise-streams", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsRemoteSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_optimized_remote_sender";
        testFullRepairRemoteSender(keyspace, cluster, 3, true, "repair", "--optimise-streams", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedDuplicateSender() throws IOException
    {
        String keyspace = "full_repair_shard_aligned_duplicate_sender";
        testFullRepairDuplicateSender(keyspace, cluster, KEY_100, 3, false, "repair", "--start-token", SHARD_ALIGNED_RANGE_1.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_1.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsDuplicateSender() throws IOException
    {
        String keyspace = "full_repair_duplicate_sender";
        testFullRepairDuplicateSender(keyspace, cluster, KEY_100, 3, false, "repair", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedDuplicateSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_optimized_aligned_duplicate_sender";
        testFullRepairDuplicateSender(keyspace, cluster, KEY_100, 6, true, "repair", "--optimise-streams", "--start-token", SHARD_ALIGNED_RANGE_1.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_1.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsDuplicateSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_optimized_duplicate_sender";
        testFullRepairDuplicateSender(keyspace, cluster, KEY_100, 6, true, "repair", "--optimise-streams", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedMultiSender() throws IOException
    {
        String keyspace = "full_repair_shard_aligned_multi_sender";
        testFullRepairMultiSender(keyspace, cluster, 3, false, "repair", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsMultiSender() throws IOException
    {
        String keyspace = "full_repair_multi_sender";
        testFullRepairMultiSender(keyspace, cluster, 3, false, "repair", "--full", keyspace);
    }

    @Test
    public void testFullRepairShardAlignedMultiSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_aligned_optimized_multi_sender";
        testFullRepairMultiSender(keyspace, cluster, 6, true, "repair", "--optimise-streams", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairAcrossShardsMultiSenderOptimized() throws IOException
    {
        String keyspace = "full_repair_optimized_multi_sender";
        testFullRepairMultiSender(keyspace, cluster, 6, true, "repair", "--optimise-streams", "--full", keyspace);
    }

    @Test
    public void testFullRepairMultiSenderSameTokenShardAligned() throws IOException
    {
        String keyspace = "full_repair_multi_sender_same_token_aligned";
        testFullRepairMultiSenderSameToken(keyspace, cluster, 3, false, "repair", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairMultiSenderSameTokenAcrossShards() throws IOException
    {
        String keyspace = "full_repair_multi_sender_same_token";
        testFullRepairMultiSenderSameToken(keyspace, cluster, 3, false, "repair", "--full", keyspace);
    }

    @Test
    public void testFullRepairMultiSenderSameTokenShardAlignedOptimized() throws IOException
    {
        String keyspace = "full_multi_sender_same_token_aligned_optimized";
        testFullRepairMultiSenderSameToken(keyspace, cluster, 6, true, "repair", "--optimise-streams", "--start-token", SHARD_ALIGNED_RANGE_2.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_2.right.toString(), "--full", keyspace);
    }

    @Test
    public void testFullRepairMultiSenderSameTokenAcrossShardsOptimized() throws IOException
    {
        String keyspace = "full_multi_sender_same_token_optimized";
        testFullRepairMultiSenderSameToken(keyspace, cluster, 6, true, "repair", "--optimise-streams", "--full", keyspace);
    }
}
