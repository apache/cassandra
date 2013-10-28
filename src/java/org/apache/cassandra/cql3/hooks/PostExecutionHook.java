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
package org.apache.cassandra.cql3.hooks;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Run after the CQL Statement is executed in
 * {@link org.apache.cassandra.cql3.QueryProcessor}.
 */
public interface PostExecutionHook
{
    /**
     * Perform post-processing on a CQL statement directly after
     * it being executed by the QueryProcessor.
     *
     * @param statement the statement to perform post-processing on
     * @param context execution context containing additional info
     *                about the operation and statement
     * @throws RequestExecutionException, RequestValidationException
     */
    void processStatement(CQLStatement statement, ExecutionContext context) throws RequestExecutionException, RequestValidationException;

    /**
     * Perform post-processing on a CQL batch directly after
     * it being executed by the QueryProcessor.
     *
     * @param batch the CQL batch to perform post-processing on
     * @param context execution context containing additional info
     *                about the operation and batch
     * @throws RequestExecutionException, RequestValidationException
     */
    void processBatch(BatchStatement batch, BatchExecutionContext context) throws RequestExecutionException, RequestValidationException;
}
