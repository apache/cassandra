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

import java.net.InetAddress;

/**
 * Interface meant to track the back-pressure state per replica host.
 */
public interface BackPressureState
{
    /**
     * Called when a message is sent to a replica.
     */
    void onMessageSent(MessageOut<?> message);

    /**
     * Called when a response is received from a replica.
     */
    void onResponseReceived();

    /**
     * Called when no response is received from replica.
     */
    void onResponseTimeout();

    /**
     * Gets the current back-pressure rate limit.
     */
    double getBackPressureRateLimit();

    /**
     * Returns the host this state refers to.
     */
    InetAddress getHost();
}
