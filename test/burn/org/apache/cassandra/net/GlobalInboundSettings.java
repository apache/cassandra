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

package org.apache.cassandra.net;

class GlobalInboundSettings
{
    final int queueCapacity;
    final long endpointReserveLimit;
    final long globalReserveLimit;
    final InboundConnectionSettings template;

    GlobalInboundSettings()
    {
        this(0, 0, 0, null);
    }

    GlobalInboundSettings(int queueCapacity, long endpointReserveLimit, long globalReserveLimit, InboundConnectionSettings template)
    {
        this.queueCapacity = queueCapacity;
        this.endpointReserveLimit = endpointReserveLimit;
        this.globalReserveLimit = globalReserveLimit;
        this.template = template;
    }

    GlobalInboundSettings withQueueCapacity(int queueCapacity)
    {
        return new GlobalInboundSettings(queueCapacity, endpointReserveLimit, globalReserveLimit, template);
    }
    GlobalInboundSettings withEndpointReserveLimit(int endpointReserveLimit)
    {
        return new GlobalInboundSettings(queueCapacity, endpointReserveLimit, globalReserveLimit, template);
    }
    GlobalInboundSettings withGlobalReserveLimit(int globalReserveLimit)
    {
        return new GlobalInboundSettings(queueCapacity, endpointReserveLimit, globalReserveLimit, template);
    }
    GlobalInboundSettings withTemplate(InboundConnectionSettings template)
    {
        return new GlobalInboundSettings(queueCapacity, endpointReserveLimit, globalReserveLimit, template);
    }
}
