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
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Back-pressure algorithm interface.
 * <br/>
 * For experts usage only. Implementors must provide a constructor accepting a single {@code Map<String, Object>} argument,
 * representing any parameters eventually required by the specific implementation.
 */
public interface BackPressureStrategy<S extends BackPressureState>
{
    /**
     * Applies the back-pressure algorithm, based and acting on the given {@link BackPressureState}s, and up to the given
     * timeout.
     */
    void apply(Set<S> states, long timeout, TimeUnit unit);

    /**
     * Creates a new {@link BackPressureState} initialized as needed by the specific implementation.
     */
    S newState(InetAddress host);
}
