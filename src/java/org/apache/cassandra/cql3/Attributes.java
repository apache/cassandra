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

import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
public class Attributes
{
    public Long timestamp;
    public int timeToLive;

    public void validate() throws InvalidRequestException
    {
        if (timeToLive < 0)
            throw new InvalidRequestException("A TTL must be greater or equal to 0");

        if (timeToLive > ExpiringColumn.MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", timeToLive, ExpiringColumn.MAX_TTL));
    }
}
