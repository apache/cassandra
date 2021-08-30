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
package org.apache.cassandra.service.reads.trackwarnings;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ParamType;

public class WarningContext
{
    private static EnumSet<ParamType> SUPPORTED = EnumSet.of(ParamType.TOMBSTONE_WARNING, ParamType.TOMBSTONE_ABORT,
                                                             ParamType.LOCAL_READ_SIZE_WARN, ParamType.LOCAL_READ_SIZE_ABORT,
                                                             ParamType.ROW_INDEX_SIZE_WARN, ParamType.ROW_INDEX_SIZE_ABORT);

    final WarnAbortCounter tombstones = new WarnAbortCounter();
    final WarnAbortCounter localReadSize = new WarnAbortCounter();
    final WarnAbortCounter rowIndexTooLarge = new WarnAbortCounter();

    public static boolean isSupported(Set<ParamType> keys)
    {
        return !Collections.disjoint(keys, SUPPORTED);
    }

    public RequestFailureReason updateCounters(Map<ParamType, Object> params, InetAddressAndPort from)
    {
        for (Map.Entry<ParamType, Object> entry : params.entrySet())
        {
            WarnAbortCounter counter = null;
            RequestFailureReason reason = null;
            switch (entry.getKey())
            {
                case ROW_INDEX_SIZE_ABORT:
                    reason = RequestFailureReason.READ_SIZE;
                case ROW_INDEX_SIZE_WARN:
                    counter = rowIndexTooLarge;
                    break;
                case LOCAL_READ_SIZE_ABORT:
                    reason = RequestFailureReason.READ_SIZE;
                case LOCAL_READ_SIZE_WARN:
                    counter = localReadSize;
                    break;
                case TOMBSTONE_ABORT:
                    reason = RequestFailureReason.READ_TOO_MANY_TOMBSTONES;
                case TOMBSTONE_WARNING:
                    counter = tombstones;
                    break;
            }
            if (reason != null)
            {
                counter.addAbort(from, ((Number) entry.getValue()).longValue());
                return reason;
            }
            if (counter != null)
                counter.addWarning(from, ((Number) entry.getValue()).longValue());
        }
        return null;
    }

    public WarningsSnapshot snapshot()
    {
        return WarningsSnapshot.create(tombstones.snapshot(), localReadSize.snapshot(), rowIndexTooLarge.snapshot());
    }
}
