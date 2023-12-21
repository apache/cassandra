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
package org.apache.cassandra.guardrails;

import org.apache.cassandra.db.Keyspace;

/**
 * A default implementation of the UserKeyspaceFilter, which just includes all keyspaces automatically.
 * This is done so that "max table count" guardrails will still work in C* databases that don't have
 * keyspaces prefixed by tenant values.
 */
public class DefaultUserKeyspaceFilter implements UserKeyspaceFilter
{
    public boolean filter(Keyspace keyspace)
    {
        return true;
    }
}
