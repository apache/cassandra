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

package org.apache.cassandra.cql3;

import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface QueryInterceptor
{
    /**
     * Intercept a statement and process it if necessary. If the interceptor processes the statement, it
     * returns a {@link ResultMessage}, otherwise returns <code>null</code>.
     *
     * @param statement
     * @param queryState
     * @param options
     * @param queryStartNanoTime
     * @return a {@link ResultMessage} if the statement is processed by the interceptor,
     * otherwise returns <code>null</code>
     */
    @Nullable
    ResultMessage interceptStatement(CQLStatement statement,
                                     QueryState queryState,
                                     QueryOptions options,
                                     long queryStartNanoTime);
}
