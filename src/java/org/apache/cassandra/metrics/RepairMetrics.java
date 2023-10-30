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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairMessage;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RepairMetrics
{
    public static final String TYPE_NAME = "Repair";
    public static final Counter previewFailures = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "PreviewFailures", null));
    public static final Histogram retries = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE_NAME, "Retries", null), false);
    public static final Map<Verb, Histogram> retriesByVerb;
    public static final Counter retryTimeout = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryTimeout", null));
    public static final Map<Verb, Counter> retryTimeoutByVerb;
    public static final Counter retryFailure = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryFailure", null));
    public static final Map<Verb, Counter> retryFailureByVerb;

    static
    {
        Map<Verb, Histogram> retries = new EnumMap<>(Verb.class);
        Map<Verb, Counter> timeout = new EnumMap<>(Verb.class);
        Map<Verb, Counter> failure = new EnumMap<>(Verb.class);
        for (Verb verb : RepairMessage.ALLOWS_RETRY)
        {
            retries.put(verb, Metrics.histogram(DefaultNameFactory.createMetricName(TYPE_NAME, "Retries-" + verb.name(), null), false));
            timeout.put(verb, Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryTimeout-" + verb.name(), null)));
            failure.put(verb, Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "RetryFailure-" + verb.name(), null)));
        }
        retriesByVerb = Collections.unmodifiableMap(retries);
        retryTimeoutByVerb = Collections.unmodifiableMap(timeout);
        retryFailureByVerb = Collections.unmodifiableMap(failure);
    }

    public static void init()
    {
        // noop
    }

    @VisibleForTesting
    public static void unsafeReset()
    {
        reset(previewFailures);
        reset(retries);
        retriesByVerb.values().forEach(RepairMetrics::reset);
        reset(retryTimeout);
        retryTimeoutByVerb.values().forEach(RepairMetrics::reset);
        reset(retryFailure);
        retryFailureByVerb.values().forEach(RepairMetrics::reset);
    }

    private static void reset(Histogram retries)
    {
        ((ClearableHistogram) retries).clear();
    }

    private static void reset(Counter counter)
    {
        counter.dec(counter.getCount());
    }

    public static void retry(Verb verb, int attempt)
    {
        retries.update(attempt);
        retriesByVerb.get(verb).update(attempt);
    }

    public static void retryTimeout(Verb verb)
    {
        retryTimeout.inc();
        retryTimeoutByVerb.get(verb).inc();
    }

    public static void retryFailure(Verb verb)
    {
        retryFailure.inc();
        retryFailureByVerb.get(verb).inc();
    }
}
