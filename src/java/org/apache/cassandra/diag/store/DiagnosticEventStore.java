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

package org.apache.cassandra.diag.store;

import java.util.NavigableMap;

import org.apache.cassandra.diag.DiagnosticEvent;

/**
 * Enables storing and retrieving {@link DiagnosticEvent}s.
 * @param <T> type of key that is used to reference an event
 */
public interface DiagnosticEventStore<T extends Comparable<T>>
{
    /**
     * Initializes the store.
     */
    void load();

    /**
     * Stores provided event and returns the new associated store key for it.
     */
    void store(DiagnosticEvent event);

    /**
     * Returns a view on all events with a key greater than the provided value (inclusive) up to the specified
     * number of results. Events may be added or become unavailable over time. Keys must be unique, sortable and
     * monotonically incrementing. Returns an empty map in case no events could be found.
     */
    NavigableMap<T, DiagnosticEvent> scan(T key, int limit);

    /**
     * Returns the greatest event ID that can be used to fetch events via {@link #scan(Comparable, int)}.
     */
    T getLastEventId();
}
