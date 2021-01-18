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
package org.apache.cassandra.diag;

import java.io.Serializable;
import java.util.Map;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Base class for internally emitted events used for diagnostics and testing.
 */
public abstract class DiagnosticEvent
{
    /**
     * Event creation time.
     */
    public final long timestamp = currentTimeMillis();

    /**
     * Name of allocating thread.
     */
    public final String threadName = Thread.currentThread().getName();

    /**
     * Returns event type discriminator. This will usually be a enum value.
     */
    public abstract Enum<?> getType();

    /**
     * Returns map of key-value pairs containing relevant event details. Values can be complex objects like other
     * maps, but must be Serializable, as returned values may be consumed by external clients. It's strongly recommended
     * to stick to standard Java classes to avoid distributing custom classes to clients and also prevent potential
     * class versioning conflicts.
     */
    public abstract Map<String, Serializable> toMap();
}
