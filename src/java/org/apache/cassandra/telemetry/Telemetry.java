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

package org.apache.cassandra.telemetry;

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.membership.Location;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ServerAttributes;
import io.opentelemetry.semconv.ServiceAttributes;

/**
 * Holds references to OpenTelemetry objects
 */
public final class Telemetry
{
    private static volatile OpenTelemetry otel;

    public static OpenTelemetry init(String clusterName,
                                     String cassandraVersion,
                                     String listenAddress,
                                     int port,
                                     String nodeId,
                                     Location location)
    {
        if (otel != null) return otel;
        synchronized (Telemetry.class)
        {
            if (otel != null) return otel;

            if (DatabaseDescriptor.getOpenTelemetryEnabled())
            {
                Resource cassandraResource = Resource.builder()
                                                     .put(ServiceAttributes.SERVICE_NAME, clusterName)
                                                     .put(ServiceAttributes.SERVICE_NAMESPACE, "cassandra")
                                                     .put(ServiceAttributes.SERVICE_VERSION, cassandraVersion)
                                                     .put(ServiceAttributes.SERVICE_INSTANCE_ID, nodeId)
                                                     .put(ServerAttributes.SERVER_ADDRESS, listenAddress)
                                                     .put(ServerAttributes.SERVER_PORT, port)
                                                     .put(CassandraAttributes.CASSANDRA_DC, location.datacenter)
                                                     .put(CassandraAttributes.CASSANDRA_RACK, location.rack)
                                                     .build();
                otel = AutoConfiguredOpenTelemetrySdk.builder()
                                                     .addPropertiesCustomizer((config) -> {
                                                         Map<String, String> customConfig = new HashMap<>();
                                                         // Disable metrics and log export for now
                                                         customConfig.put("otel.metrics.exporter", "none");
                                                         customConfig.put("otel.logs.exporter", "none");
                                                         return customConfig;
                                                     })
                                                     .addResourceCustomizer((r, config) -> r.merge(cassandraResource))
                                                     .build()
                                                     .getOpenTelemetrySdk();
            }
            else
            {
                otel = OpenTelemetry.noop();
            }
            return otel;
        }
    }

    private Telemetry()
    {
    }

    @VisibleForTesting
    static void setOpenTelemetryUnsafe(OpenTelemetry openTelemetry)
    {
        otel = openTelemetry;
    }

    /**
     * Returns OpenTelemetry {@link Tracer} to trace client requests.
     * Safe to call before `init` completes; returns noop tracer until initialization.
     *
     * @return Client request {@link Tracer}
     */
    public static Tracer getRequestTracer()
    {
        OpenTelemetry current = otel;
        if (current == null)
            current = OpenTelemetry.noop();
        return current.getTracer("org.apache.cassandra.request");
    }
}
