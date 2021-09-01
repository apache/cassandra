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

package org.apache.cassandra.simulator.systems;

import javax.annotation.Nullable;

import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface InterceptorOfWaits
{
    /**
     * If this interceptor is debugging wait/wake/now sites, return one initialised with the current trace of the
     * provided thread; otherwise return null.
     */
    @Nullable CaptureSites captureWaitSite(Thread thread);

    /**
     * Returns the current thread as an InterceptibleThread IF it has its InterceptConsequences interceptor set.
     * Otherwise, one of the following will happen:
     *   * if the InterceptorOfWaits permits it, null will be returned;
     *   * if it does not, the process will be failed.
     */
    @Nullable InterceptibleThread ifIntercepted();

    void interceptSignal(Thread signalledBy, InterceptedWait signalled, CaptureSites waitSites, InterceptorOfConsequences interceptedBy);
}
