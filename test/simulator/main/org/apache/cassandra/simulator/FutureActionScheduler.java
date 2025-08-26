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

package org.apache.cassandra.simulator;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;

import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER_AND_TIMEOUT;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.FAILURE;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.TIMEOUT;

/**
 * Makes decisions about when in the simulated scheduled, in terms of the global simulated nanoTime,
 * events should occur.
 */
public interface FutureActionScheduler
{
    enum Deliver { DELIVER, TIMEOUT, DELIVER_AND_TIMEOUT, FAILURE }

    class DeliverResult
    {
        public final Deliver deliver;
        // A message that should get a better than normal treatment in terms of unreliability
        public final boolean protectedMessage;

        public DeliverResult(Deliver deliver, boolean protectedMessage)
        {
            this.deliver = deliver;
            this.protectedMessage = protectedMessage;
        }
    }

    DeliverResult DELIVER_UNPROTECTED_RESULT = new DeliverResult(DELIVER, false);
    DeliverResult TIMEOUT_RESULT = new DeliverResult(TIMEOUT, false);
    DeliverResult DELIVER_AND_TIMEOUT_RESULT = new DeliverResult(DELIVER_AND_TIMEOUT, false);
    DeliverResult FAILURE_RESULT = new DeliverResult(FAILURE, false);

    /**
     * Make a decision about the result of some attempt to deliver a message.
     * Note that this includes responses, so for any given message the chance
     * of a successful reply depends on two of these calls succeeding.
     */
    DeliverResult shouldDeliver(int from, int to, IInvokableInstance invoker, IMessage message);

    /**
     * The simulated global nanoTime arrival of a message
     */
    long messageDeadlineNanos(int from, int to, boolean protectedMessage);

    /**
     * The simulated global nanoTime at which a timeout should be reported for a message
     * with {@code expiresAfterNanos} timeout
     */
    long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos, boolean protectedMessage);

    /**
     * The simulated global nanoTime at which a failure should be reported for a message
     */
    long messageFailureNanos(int from, int to, boolean protectedMessage);

    /**
     * The additional time in nanos that should elapse for some thread signal event to occur
     * to simulate scheduler latency
     */
    long schedulerDelayNanos();
}
