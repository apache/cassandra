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

package org.apache.cassandra.distributed.test.accord.load;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.Feature;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOGBACK_CONFIGURATION_FILE;
import static org.apache.cassandra.distributed.test.accord.load.LoadSettings.ycsbZipfian;

public class AccordRebootstrapLoadTest extends AccordLoadTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordRebootstrapLoadTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    public void setupCluster(int nodeCount)
    {
        LOGBACK_CONFIGURATION_FILE.setString("test/conf/logback-dtest-info.xml");
        setupCluster(nodeCount, config -> {
            config.with(Feature.NETWORK, Feature.GOSSIP)
                  .set("accord.shard_durability_target_splits", "8")
                  .set("accord.shard_durability_max_splits", "16")
                  .set("accord.shard_durability_cycle", "1m")
                  .set("accord.queue_submission_model", "SIGNAL")
                  .set("accord.command_store_shard_count", "8")
                  .set("accord.queue_thread_count", "4")
                  .set("accord.queue_shard_count", "1")
                  .set("accord.catchup_on_start_fail_latency", "2m")
                  // repair retries are disabled by default (RetrySpec.DEFAULT_MAX_ATTEMPTS == DISABLED), which makes a
                  // single lost VALIDATION_RSP/SYNC_RSP strand the repair - and therefore the rebootstrap that waits on
                  // it - forever. Accord relies on repair for its bootstrap data fetch, so enable them here.
                  .set("repair.retries.max_attempts", "3")
                  .set("repair.retries.base_sleep_time", "200ms")
                  .set("repair.retries.max_sleep_time", "1s")
                  .set("repair.retries.merkle_tree_response.base_sleep_time", "5s")
                  .set("repair.retries.merkle_tree_response.max_sleep_time", "30s");
        });
    }

    @Test
    public void testLoad() throws Exception
    {
        testLoad(new LoadSettings.Builder()
                 .setKeySelector(ycsbZipfian(100_000))
                 .setRatePerSecond(200)
                 .setClusterChaosInterval(2000)
                 .setClusterChaosConcurrency(2)
                 .setTotalClusterChaos(5)
                 .build());
    }
}
