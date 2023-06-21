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

package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;

import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Utility used to deserialize function arguments.
 */
@FunctionalInterface
public interface ArgumentDeserializer
{
    /**
     * An {@link ArgumentDeserializer} that do not deserialize data.
     */
    ArgumentDeserializer NOOP_DESERIALIZER = (protocolVersion, buffer) -> buffer;

    /**
     * Deserializes the specified function argument.
     *
     * @param protocolVersion the protocol version
     * @param buffer the serialized argument
     * @return the deserialized argument
     */
    Object deserialize(ProtocolVersion protocolVersion, ByteBuffer buffer);
}
