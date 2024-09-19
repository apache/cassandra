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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.utils.Closeable;

import static accord.utils.Property.qt;

public class HappyPathFuzzTest extends FuzzTestBase
{
    @Test
    public void happyPath()
    {
        // disable all retries, no delays/drops are possible
        DatabaseDescriptor.getRepairRetrySpec().maxAttempts = RetrySpec.MaxAttempt.DISABLED;
        Map<String, LongArrayList> repairTypeRuntimes = new HashMap<>();
        long realStartNanos = System.nanoTime();
        qt().withPure(false).withExamples(10).check(rs -> {
            Cluster cluster = new Cluster(rs);
            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node coordinator = coordinatorGen.next(rs);

                long nowNanos = System.nanoTime();
                RepairCoordinator repair = coordinator.repair(KEYSPACE, repairOption(rs, coordinator, KEYSPACE, TABLES));
                repair.run();
                boolean shouldSync = rs.nextBoolean();
                if (shouldSync)
                    closeables.add(cluster.nodes.get(pickParticipant(rs, coordinator, repair)).doValidation((cfs, validator) -> addMismatch(rs, cfs, validator)));

                runAndAssertSuccess(cluster, example, shouldSync, repair);
                repairTypeRuntimes.computeIfAbsent(repair.state.getType(), ignore -> new LongArrayList()).addLong(System.nanoTime() - nowNanos);
                closeables.forEach(Closeable::close);
                closeables.clear();
            }
        });
        long realDurationNanos = System.nanoTime() - realStartNanos;
        long repairDurationsNanos = 0;
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, LongArrayList> e : repairTypeRuntimes.entrySet())
        {
            sb.append(e.getKey());
            long[] times = e.getValue().toLongArray();
            repairDurationsNanos += LongStream.of(times).sum();
            Arrays.sort(times);
            long min = times[0];
            long median = times[times.length / 2];
            long max = times[times.length - 1];
            sb.append(": min=").append(TimeUnit.NANOSECONDS.toMillis(min))
              .append(", median=").append(TimeUnit.NANOSECONDS.toMillis(median))
              .append(", max=").append(TimeUnit.NANOSECONDS.toMillis(max))
              .append(", count=").append(times.length)
              .append('\n');
        }
        logger.info("Repair runtimes (in millis):\nTest Duration {}\nRepair Duration {}\n{}", TimeUnit.NANOSECONDS.toMillis(realDurationNanos), TimeUnit.NANOSECONDS.toMillis(repairDurationsNanos), sb);
    }
}
