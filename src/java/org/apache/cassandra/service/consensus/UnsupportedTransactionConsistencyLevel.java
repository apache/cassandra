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

package org.apache.cassandra.service.consensus;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.accord.IAccordService;

public class UnsupportedTransactionConsistencyLevel extends InvalidRequestException
{
    public enum Kind { Read, Commit }

    private UnsupportedTransactionConsistencyLevel(Kind kind, ConsistencyLevel consistencyLevel)
    {
        super("ConsistencyLevel " + consistencyLevel + " is unsupported with Accord for " + (kind == Kind.Commit ? "write/commit" : "read") + ", supported are " + (kind == Kind.Commit ? IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS : IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS));
    }

    public static UnsupportedTransactionConsistencyLevel commit(ConsistencyLevel consistencyLevel)
    {
        return new UnsupportedTransactionConsistencyLevel(Kind.Commit, consistencyLevel);
    }

    public static UnsupportedTransactionConsistencyLevel read(ConsistencyLevel consistencyLevel)
    {
        return new UnsupportedTransactionConsistencyLevel(Kind.Read, consistencyLevel);
    }
}
