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

/**
 * <h1>Non-blocking streaming with netty</h1>
 * This document describes the implementation details of the streaming protocol. A listener for a streaming
 * session listens on the same socket as internode messaging, and participates in the same handshake protocol
 * That protocol is described in the package-level documentation for {@link org.apache.cassandra.net.async}, and
 * thus not here.
 *
 * Streaming 2.0 was implemented as CASSANDRA-5286. Streaming 2.0 used (the equivalent of) a single thread and
 * a single socket to transfer sstables sequentially to a peer (either as part of a repair, bootstrap, and so on).
 * Part of the motivation for switching to netty and a non-blocking model as to enable stream transfers to occur
 * in parallel for a given session.
 *
 * Thus, a more detailed approach is required for stream session management.
 *
 * <h2>Session setup and management</h2>
 *
 * The full details of the session lifecycle are documented in {@link org.apache.cassandra.streaming.StreamSession}.
 *
 */
package org.apache.cassandra.streaming.async;

