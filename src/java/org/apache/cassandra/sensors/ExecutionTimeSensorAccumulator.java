/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.sensors;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Per-request accumulator for execution-time sensors.
 *
 * <p>Execution-time sensors require two-level aggregation:
 * <ul>
 *   <li><em>Within a phase</em>: replicas execute in parallel, so only the slowest replica's time
 *       matters — accumulated as a running {@code max} per {@code (context, type)} pair.
 *       Please note this only takes into account as many replicas as required by the given consistency level.</li>
 *   <li><em>Across sequential phases</em> (e.g. CAS prepare → propose → commit): phases are
 *       sequential from the coordinator's perspective, so each phase max must be <em>summed</em>.</li>
 * </ul>
 * </p>
 *
 * <p>Usage: for each replica response, call {@link #accumulate} once per {@code (context, type)}
 * pair reported by that replica (e.g. once per table in a multi-table mutation), then call
 * {@link #onResponse} exactly once to count the response. When {@code onResponse} has been called
 * {@code threshold} times, {@code incrementSensor} is invoked for every accumulated
 * {@code (context, type)} with its running max value.</p>
 */
public class ExecutionTimeSensorAccumulator
{
    private final int threshold;
    private final AtomicInteger responseCount = new AtomicInteger(0);

    /** Running max per (context, type) pair — populated by accumulate(). */
    private final ConcurrentHashMap<ContextTypePair, AtomicDouble> maxByContextType = new ConcurrentHashMap<>();

    /**
     * @param threshold number of {@link #onResponse} calls after which the accumulated maxes are
     *                  written to the sensors.
     */
    public ExecutionTimeSensorAccumulator(int threshold)
    {
        this.threshold = threshold;
    }

    /**
     * Updates the running max for the given {@code (context, type)} pair with {@code value}.
     * Does <em>not</em> increment the response count — call {@link #onResponse} once per replica
     * response to do that.
     *
     * @param context sensor context (table / keyspace)
     * @param type    the execution-time sensor type
     * @param value   the execution-time value reported by this replica for this context
     */
    public void accumulate(Context context, Type type, double value)
    {
        maxByContextType.computeIfAbsent(new ContextTypePair(context, type), k -> new AtomicDouble(0))
                        .updateAndGet(current -> Math.max(value, current));
    }

    /**
     * Counts one replica response. When the count reaches {@code threshold}, calls
     * {@code incrementSensor} for every {@code (context, type)} pair that was registered via
     * {@link #accumulate}, using the running max value for each.
     *
     * @param sensors the request sensors for this in-flight request (may be {@code null} — no-op)
     */
    public void onResponse(RequestSensors sensors)
    {
        if (sensors == null)
            return;

        if (responseCount.incrementAndGet() == threshold)
        {
            for (ConcurrentHashMap.Entry<ContextTypePair, AtomicDouble> entry : maxByContextType.entrySet())
                sensors.incrementSensor(entry.getKey().context, entry.getKey().type, entry.getValue().get());
        }
    }

    /**
     * Returns true if the response threshold has been reached, that is if {@link #onResponse(RequestSensors)}
     * has been invoked enough times to trigger the sensor increment with the max.
     */
    public boolean isResponseThresholdReached()
    {
        return responseCount.get() >= threshold;
    }

    private static final class ContextTypePair
    {
        final Context context;
        final Type type;

        ContextTypePair(Context context, Type type)
        {
            this.context = context;
            this.type = type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof ContextTypePair)) return false;
            ContextTypePair that = (ContextTypePair) o;
            return context.equals(that.context) && type == that.type;
        }

        @Override
        public int hashCode()
        {
            return 31 * context.hashCode() + type.hashCode();
        }
    }
}
