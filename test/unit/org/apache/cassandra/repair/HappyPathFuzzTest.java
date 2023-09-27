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
import java.util.List;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
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
        qt().withPure(false).withExamples(10).check(rs -> {
            Cluster cluster = new Cluster(rs);
            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node coordinator = coordinatorGen.next(rs);

                RepairCoordinator repair = coordinator.repair(KEYSPACE, repairOption(rs, coordinator, KEYSPACE, TABLES));
                repair.run();
                boolean shouldSync = rs.nextBoolean();
                if (shouldSync)
                    closeables.add(cluster.nodes.get(pickParticipant(rs, coordinator, repair)).doValidation((cfs, validator) -> addMismatch(rs, cfs, validator)));

                runAndAssertSuccess(cluster, example, shouldSync, repair);
                closeables.forEach(Closeable::close);
                closeables.clear();
            }
        });
    }
}
