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

package org.apache.cassandra.service.writes.thresholds;


import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ParamType;

/**
 * Accumulates write warning information from replica responses.
 * Similar to WarningContext but for write operations (warnings only, no aborts).
 */
public class WriteWarningContext
{
    private static final EnumSet<ParamType> SUPPORTED = EnumSet.of(
        ParamType.WRITE_SIZE_WARN,
        ParamType.WRITE_TOMBSTONE_WARN
    );

    final WarnCounter writeSize = new WarnCounter();
    final WarnCounter writeTombstone = new WarnCounter();

    public static boolean isSupported(Set<ParamType> keys)
    {
        return !Collections.disjoint(keys, SUPPORTED);
    }

    /**
     * Update counters from replica response parameters.
     * Writes never abort, so this always returns without throwing.
     */
    public void updateCounters(Map<ParamType, Object> params, InetAddressAndPort from)
    {
        for (Map.Entry<ParamType, Object> entry : params.entrySet())
        {
            WarnCounter counter = null;
            switch (entry.getKey())
            {
                case WRITE_SIZE_WARN:
                    counter = writeSize;
                    break;
                case WRITE_TOMBSTONE_WARN:
                    counter = writeTombstone;
                    break;
            }

            if (counter != null)
                counter.addWarning(from, ((Number) entry.getValue()).longValue());
        }
    }

    public WriteWarningsSnapshot snapshot()
    {
        return WriteWarningsSnapshot.create(
        writeSize.snapshot(),
        writeTombstone.snapshot()
        );
    }
}
