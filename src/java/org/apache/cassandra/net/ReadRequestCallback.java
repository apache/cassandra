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
package org.apache.cassandra.net;

import org.apache.cassandra.sensors.Context;

/**
 * Marker interface for read-path {@link RequestCallback} implementations that expose the sensor
 * {@link Context} for their associated read command. Implemented by both
 * {@link org.apache.cassandra.service.reads.ReadCallback} (single-range and single-partition reads)
 * and {@link org.apache.cassandra.service.reads.range.EndpointGroupingCoordinator.EndpointQueryContext}'s
 * {@code SingleEndpointCallback} (multi-range endpoint-grouping reads), allowing
 * {@link ResponseVerbHandler} to apply uniform sensor tracking logic for all read responses
 * regardless of the specific callback type.
 */
public interface ReadRequestCallback<T> extends RequestCallback<T>
{
    Context sensorsContext();
}
