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

package org.apache.cassandra.service.accord;

import accord.local.Node;
import accord.utils.Invariants;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Maps network addresses to accord ids
 */
public interface AccordEndpointMapper
{
    Node.Id mappedIdOrNull(InetAddressAndPort endpoint);
    InetAddressAndPort mappedEndpointOrNull(Node.Id id);

    default Node.Id mappedId(InetAddressAndPort endpoint)
    {
        return Invariants.nonNull(mappedIdOrNull(endpoint), "Unable to map address %s to a Node.Id", endpoint);
    }

    default InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        return Invariants.nonNull(mappedEndpointOrNull(id), "Unable to map node id %s to a InetAddressAndPort", id);
    }
}
