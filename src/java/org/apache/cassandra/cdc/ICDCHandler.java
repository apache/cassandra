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

package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface ICDCHandler
{

    /**
     * Initialize the handler with specific options passed by the user.
     * @param options  options that are defined in TableParams (with CQL)
     * @throws ConfigurationException
     */
    public void initialize(Map<String, String> options) throws ConfigurationException;

    /**
     * Process the cdc data, throw IOException if the data is unprocessed. Base on retry policy,
     * CDC will save the data in a seperated (compacted) CDC log file and retry later.
     * @param mutation  cdc log mutation
     * @throws IOException  If the mutation is failed to process
     */
    public void process(Mutation mutation) throws IOException;
}
