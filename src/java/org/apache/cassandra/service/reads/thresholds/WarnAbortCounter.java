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
package org.apache.cassandra.service.reads.thresholds;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.locator.InetAddressAndPort;

public class WarnAbortCounter
{
    final Set<InetAddressAndPort> warnings = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // the highest number reported by a node's warning
    final AtomicLong maxWarningValue = new AtomicLong();

    final Set<InetAddressAndPort> aborts = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // the highest number reported by a node's rejection.
    final AtomicLong maxAbortsValue = new AtomicLong();

    void addWarning(InetAddressAndPort from, long value)
    {
        maxWarningValue.accumulateAndGet(value, Math::max);
        // call add last so concurrent reads see empty even if values > 0; if done in different order then
        // size=1 could have values == 0
        warnings.add(from);
    }

    void addAbort(InetAddressAndPort from, long value)
    {
        maxAbortsValue.accumulateAndGet(value, Math::max);
        // call add last so concurrent reads see empty even if values > 0; if done in different order then
        // size=1 could have values == 0
        aborts.add(from);
    }

    public WarningsSnapshot.Warnings snapshot()
    {
        return WarningsSnapshot.Warnings.create(WarningsSnapshot.Counter.create(warnings, maxWarningValue), WarningsSnapshot.Counter.create(aborts, maxAbortsValue));
    }
}
