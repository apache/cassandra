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
package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Repair session result
 */
public class RepairSessionResult
{
    public final UUID sessionId;
    public final String keyspace;
    public final Range<Token> range;
    public final Collection<RepairResult> repairJobResults;

    public RepairSessionResult(UUID sessionId, String keyspace, Range<Token> range, Collection<RepairResult> repairJobResults)
    {
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.range = range;
        this.repairJobResults = repairJobResults;
    }
}
