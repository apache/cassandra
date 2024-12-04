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

package org.apache.cassandra.simulator.paxos;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.LogResult;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

public class AbstractPairOfSequencesPaxosSimulationTest
{
    @Test
    public void parseSuccess()
    {
        String log = "WARN  [AccordExecutor[1,0]:1] node1 2024-12-03 17:45:24,574 [10,1577851211987004,9(RX),1]: Exception coordinating ExclusiveSyncPoint for [1b255f4d-ef25-40a6-0000-000000000009:[(-2978380553567688022,-2930342157542402732]]] durability. Increased numberOfSplits to 256\n" +
                     "accord.coordinate.Invalidated: null\n" +
                     "\tat accord.coordinate.Propose$Invalidate.lambda$proposeAndCommitInvalidate$2(Propose.java:193)\n" +
                     "\tat accord.local.Node.withEpoch(Node.java:391)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.lambda$proposeAndCommitInvalidate$3(Propose.java:186)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.onSuccess(Propose.java:217)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.onSuccess(Propose.java:146)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe$RegisteredCallback.unsafeOnSuccess(RequestCallbacks.java:119)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe.lambda$onSuccess$0(RequestCallbacks.java:189)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe$RegisteredCallback.lambda$safeInvoke$0(RequestCallbacks.java:140)\n" +
                     "\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n" +
                     "\tat accord.utils.async.AsyncChains.lambda$encapsulate$0(AsyncChains.java:498)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutor$PlainRunnable.run(AccordExecutor.java:989)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutor$CommandStoreQueue.run(AccordExecutor.java:729)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutorSimple.run(AccordExecutorSimple.java:95)\n" +
                     "\tat org.apache.cassandra.concurrent.FutureTask$2.call(FutureTask.java:124)\n" +
                     "\tat org.apache.cassandra.concurrent.SyncFutureTask.run(SyncFutureTask.java:68)\n" +
                     "\tat org.apache.cassandra.simulator.systems.InterceptingExecutor$AbstractSingleThreadedExecutorPlus.lambda$new$0(InterceptingExecutor.java:585)\n" +
                     "\tat io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)\n" +
                     "\tat java.base/java.lang.Thread.run(Thread.java:829)";
        LogResult<List<String>> errors = new LogAction.BasicLogResult<>(42, Collections.singletonList(log));

        AbstractPairOfSequencesPaxosSimulation simulation = Mockito.mock(AbstractPairOfSequencesPaxosSimulation.class);
        Mockito.doCallRealMethod().when(simulation).checkErrorLogs(Mockito.eq(0), Mockito.eq(errors));
        Mockito.when(simulation.expectedExceptions()).thenReturn((Class<? extends Throwable>[]) new Class<?>[] { accord.coordinate.Invalidated.class });

        simulation.checkErrorLogs(0, errors);
    }

    @Test
    public void parseFailure()
    {
        String log = "FAKE  [AccordExecutor[1,0]:1] node1 2024-12-03 17:45:24,574 [10,1577851211987004,9(RX),1]: Exception coordinating ExclusiveSyncPoint for [1b255f4d-ef25-40a6-0000-000000000009:[(-2978380553567688022,-2930342157542402732]]] durability. Increased numberOfSplits to 256\n" +
                     "accord.coordinate.Invalidated: null\n" +
                     "\tat accord.coordinate.Propose$Invalidate.lambda$proposeAndCommitInvalidate$2(Propose.java:193)\n" +
                     "\tat accord.local.Node.withEpoch(Node.java:391)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.lambda$proposeAndCommitInvalidate$3(Propose.java:186)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.onSuccess(Propose.java:217)\n" +
                     "\tat accord.coordinate.Propose$Invalidate.onSuccess(Propose.java:146)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe$RegisteredCallback.unsafeOnSuccess(RequestCallbacks.java:119)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe.lambda$onSuccess$0(RequestCallbacks.java:189)\n" +
                     "\tat accord.impl.RequestCallbacks$CallbackStripe$RegisteredCallback.lambda$safeInvoke$0(RequestCallbacks.java:140)\n" +
                     "\tat java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)\n" +
                     "\tat accord.utils.async.AsyncChains.lambda$encapsulate$0(AsyncChains.java:498)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutor$PlainRunnable.run(AccordExecutor.java:989)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutor$CommandStoreQueue.run(AccordExecutor.java:729)\n" +
                     "\tat org.apache.cassandra.service.accord.AccordExecutorSimple.run(AccordExecutorSimple.java:95)\n" +
                     "\tat org.apache.cassandra.concurrent.FutureTask$2.call(FutureTask.java:124)\n" +
                     "\tat org.apache.cassandra.concurrent.SyncFutureTask.run(SyncFutureTask.java:68)\n" +
                     "\tat org.apache.cassandra.simulator.systems.InterceptingExecutor$AbstractSingleThreadedExecutorPlus.lambda$new$0(InterceptingExecutor.java:585)\n" +
                     "\tat io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)\n" +
                     "\tat java.base/java.lang.Thread.run(Thread.java:829)";
        LogResult<List<String>> errors = new LogAction.BasicLogResult<>(42, Collections.singletonList(log));

        AbstractPairOfSequencesPaxosSimulation simulation = Mockito.mock(AbstractPairOfSequencesPaxosSimulation.class);
        Mockito.doCallRealMethod().when(simulation).checkErrorLogs(Mockito.eq(0), Mockito.eq(errors));
        Mockito.when(simulation.expectedExceptions()).thenReturn((Class<? extends Throwable>[]) new Class<?>[] { accord.coordinate.Invalidated.class });

        Assertions.assertThatThrownBy(() -> simulation.checkErrorLogs(0, errors))
                  .isInstanceOf(AssertionError.class)
                  .hasMessageStartingWith("Saw errors in node0: Unexpected exception (could not parse line):");
    }
}