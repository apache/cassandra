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
package org.apache.cassandra.streaming.messages;

import org.apache.cassandra.streaming.LogStreamHeader;

public abstract class MutationLogStreamMessage extends StreamMessage
{
    public final LogStreamHeader header;

    protected MutationLogStreamMessage(LogStreamHeader header)
    {
        super(Type.MUTATION_LOG_STREAM);
        this.header = header;
    }

    @Override
    public String toString()
    {
        return String.format("%s{header=%s}", getClass().getSimpleName(), header);
    }
}