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

package org.apache.cassandra.harry.model;

import java.util.List;

import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.execution.ResultSetRow;
import org.apache.cassandra.harry.op.Visit;

public interface Model
{
    void validate(Operations.SelectStatement select, List<ResultSetRow> actual);

    class LtsOperationPair
    {
        final long lts;
        final int opId;

        public LtsOperationPair(long lts, int opId)
        {
            this.lts = lts;
            this.opId = opId;
        }
    }

    interface Replay extends Iterable<Visit>
    {
        Visit replay(long lts);
        Operations.Operation replay(long lts, int opId);
    }

}
