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

package org.apache.cassandra.distributed.api;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Future;

// The cross-version API requires that a Coordinator can be constructed without any constructor arguments
public interface ICoordinator
{
    // a bit hacky, but ConsistencyLevel draws in too many dependent classes, so we cannot have a cross-version
    // method signature that accepts ConsistencyLevel directly.  So we just accept an Enum<?> and cast.
    Object[][] execute(String query, Enum<?> consistencyLevel, Object... boundValues);

    Iterator<Object[]> executeWithPaging(String query, Enum<?> consistencyLevel, int pageSize, Object... boundValues);

    Future<Object[][]> asyncExecuteWithTracing(UUID sessionId, String query, Enum<?> consistencyLevel, Object... boundValues);
    Object[][] executeWithTracing(UUID sessionId, String query, Enum<?> consistencyLevel, Object... boundValues);
}
