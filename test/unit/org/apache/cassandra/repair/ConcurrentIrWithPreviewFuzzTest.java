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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.utils.Closeable;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class ConcurrentIrWithPreviewFuzzTest extends FuzzTestBase
{
    @Test
    public void concurrentIrWithPreview()
    {
        // to avoid unlucky timing issues, retry until success; given enough retries we should eventually become success
        DatabaseDescriptor.getRepairRetrySpec().maxAttempts = new RetrySpec.MaxAttempt(Integer.MAX_VALUE);
        qt().withPure(false).withExamples(1).check(rs -> {
            Cluster cluster = new Cluster(rs);
            enableMessageFaults(cluster);

            Gen<Cluster.Node> coordinatorGen = Gens.pick(cluster.nodes.keySet()).map(cluster.nodes::get);

            List<Closeable> closeables = new ArrayList<>();
            for (int example = 0; example < 100; example++)
            {
                Cluster.Node irCoordinator = coordinatorGen.next(rs);
                Cluster.Node previewCoordinator = coordinatorGen.next(rs);
                RepairCoordinator ir = irCoordinator.repair(KEYSPACE, irOption(rs, irCoordinator, KEYSPACE, ignore -> TABLES));
                ir.run();
                RepairCoordinator preview = previewCoordinator.repair(KEYSPACE, previewOption(rs, previewCoordinator, KEYSPACE, ignore -> TABLES), false);
                preview.run();

                closeables.add(cluster.nodes.get(pickParticipant(rs, previewCoordinator, preview)).doValidation(ignore -> (cfs, validator) -> addMismatch(rs, cfs, validator)));
                // cause a delay in validation to have more failing previews
                closeables.add(cluster.nodes.get(pickParticipant(rs, previewCoordinator, preview)).doValidation(next -> (cfs, validator) -> {
                    if (validator.desc.parentSessionId.equals(preview.state.id))
                        cluster.unorderedScheduled.schedule(() -> next.accept(cfs, validator), 1, TimeUnit.HOURS);
                    else next.acceptOrFail(cfs, validator);
                }));
                // make sure listeners don't leak
                closeables.add(LocalSessions::unsafeClearListeners);

                cluster.processAll();

                // IR will always pass, but preview is what may fail (if the coordinator is the same)
                Assertions.assertThat(ir.state.getResult()).describedAs("Unexpected state: %s -> %s; example %d", ir.state, ir.state.getResult(), example).isEqualTo(Completable.Result.success(repairSuccessMessage(ir)));

                Assertions.assertThat(preview.state.getResult()).describedAs("Unexpected state: %s; example %d", preview.state, example).isNotNull();

                if (irCoordinator == previewCoordinator)
                {
                    Assertions.assertThat(preview.state.getResult().message).describedAs("Unexpected state: %s -> %s; example %d", preview.state, preview.state.getResult(), example).contains("failed with error An incremental repair with session id");
                }
                else
                {
                    assertSuccess(example, true, preview);
                }
                closeables.forEach(Closeable::close);
                closeables.clear();
            }
        });
    }
}
