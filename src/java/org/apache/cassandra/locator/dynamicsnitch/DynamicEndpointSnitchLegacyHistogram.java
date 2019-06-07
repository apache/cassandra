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

package org.apache.cassandra.locator.dynamicsnitch;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

/**
 * The legacy dynamic snitching implementation (prior to 4.0) that uses Exponentially Decaying Histograms to
 * prefer or de-prefer hosts. Instead of relying on latency probes this implementation just resets
 * reservoirs for hosts we haven't talked to.
 *
 * This is left only for users that explicitly want the old behavior.
 */
public class DynamicEndpointSnitchLegacyHistogram extends DynamicEndpointSnitchHistogram
{
    // Called via reflection
    public DynamicEndpointSnitchLegacyHistogram(IEndpointSnitch snitch)
    {
        this(snitch, "legacy");
    }

    public DynamicEndpointSnitchLegacyHistogram(IEndpointSnitch snitch, String instance)
    {
        super(snitch, instance);
    }

    @VisibleForTesting
    protected Map<InetAddressAndPort, AnnotatedMeasurement> getMeasurementsWithPort()
    {
        return samples;
    }

    /**
     * Overrides the top level updateSamples method to emulate the old DES reset behavior with the minor change
     * that we only clear the samples if at least one of the ranked hosts exceeded 10 minutes of no measurements
     */
    @Override
    protected void updateSamples()
    {
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry: samples.entrySet())
        {
            AnnotatedMeasurement measurement = entry.getValue();
            long millisSinceLastMeasure = measurement.millisSinceLastMeasure.getAndAdd(dynamicSampleUpdateInterval);
            if (millisSinceLastMeasure >= MAX_PROBE_INTERVAL_MS)
            {
                samples.clear();
                return;
            }
        }
    }
}
