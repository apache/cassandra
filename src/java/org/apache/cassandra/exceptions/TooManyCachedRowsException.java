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

package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ReadCommand;

import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;

/**
 * Exception thrown when {@link org.apache.cassandra.service.ReplicaFilteringProtection} caches
 * the configured threshold number of row results from participating replicas.
 */
public class TooManyCachedRowsException extends RequestExecutionException
{
    public TooManyCachedRowsException(int threshold, ReadCommand command)
    {
        super(SERVER_ERROR, String.format("Replica filtering protection has cached over %d rows during query %s. " +
                                          "(See 'cached_replica_rows_fail_threshold' in cassandra.yaml.)",
                                          threshold, command.toCQLString()));
    }
}
