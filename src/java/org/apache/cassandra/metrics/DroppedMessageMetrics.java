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
package org.apache.cassandra.metrics;

import java.util.EnumMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.metrics.DefaultNameFactory.createMetricName;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics
{
    private static final String TYPE = "DroppedMessage";

    // backward compatibility for request metrics which names have changed in 4.0 as part of CASSANDRA-15066
    private static final ImmutableMap<Verb, String> REQUEST_VERB_ALIAS;

    static
    {
        EnumMap<Verb, String> aliases = new EnumMap<>(Verb.class);
        aliases.put(Verb.BATCH_REMOVE_REQ, "BATCH_REMOVE");
        aliases.put(Verb.BATCH_STORE_REQ, "BATCH_STORE");
        aliases.put(Verb.COUNTER_MUTATION_REQ, "COUNTER_MUTATION");
        aliases.put(Verb.HINT_REQ, "HINT");
        aliases.put(Verb.MUTATION_REQ, "MUTATION");
        aliases.put(Verb.RANGE_REQ, "RANGE_SLICE");
        aliases.put(Verb.READ_REQ, "READ");
        aliases.put(Verb.READ_REPAIR_REQ, "READ_REPAIR");
        aliases.put(Verb.REQUEST_RSP, "REQUEST_RESPONSE");

        REQUEST_VERB_ALIAS = Maps.immutableEnumMap(aliases);
    }

    /** Number of dropped messages */
    public final Meter dropped;

    /** The dropped latency within node */
    public final Timer internalDroppedLatency;

    /** The cross node dropped latency */
    public final Timer crossNodeDroppedLatency;

    public DroppedMessageMetrics(Verb verb)
    {
        String scope = verb.toString();

        if (REQUEST_VERB_ALIAS.containsKey(verb))
        {
            String alias = REQUEST_VERB_ALIAS.get(verb);
            dropped = Metrics.meter(createMetricName(TYPE, "Dropped", scope),
                                    createMetricName(TYPE, "Dropped", alias));
            internalDroppedLatency = Metrics.timer(createMetricName(TYPE, "InternalDroppedLatency", scope),
                                                   createMetricName(TYPE, "InternalDroppedLatency", alias));
            crossNodeDroppedLatency = Metrics.timer(createMetricName(TYPE, "CrossNodeDroppedLatency", scope),
                                                    createMetricName(TYPE, "CrossNodeDroppedLatency", alias));
        }
        else
        {
            dropped = Metrics.meter(createMetricName(TYPE, "Dropped", scope));
            internalDroppedLatency = Metrics.timer(createMetricName(TYPE, "InternalDroppedLatency", scope));
            crossNodeDroppedLatency = Metrics.timer(createMetricName(TYPE, "CrossNodeDroppedLatency", scope));
        }
    }
}
