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

package org.apache.cassandra.service;

import java.io.IOException;

/**
 * Interface for producing query analytics data
 * Implemented in Cassandra Mesos
 */
public interface QueryAnalyticsDataProducer {

    /**
     * Produce a single query analytics datapoint.
     * @param datapoint the analytics data as a structured object
     * @throws IOException if data production fails
     */
    void produceDatapoint(QueryAnalyticsDatapoint datapoint) throws IOException;

    default void close() {

    }
}
