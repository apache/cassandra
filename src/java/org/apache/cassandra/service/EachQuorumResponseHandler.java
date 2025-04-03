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
package org.apache.cassandra.service;

import java.util.function.Supplier;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.transport.Dispatcher;


/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class EachQuorumResponseHandler<T> extends WriteResponseHandler<T>
{
    public EachQuorumResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                     Runnable callback,
                                     WriteType writeType,
                                     Supplier<Mutation> hintOnFailure,
                                     Dispatcher.RequestTime requestTime)
    {
        super(replicaPlan, callback, writeType, hintOnFailure, requestTime);
        assert replicaPlan.consistencyLevel() == ConsistencyLevel.EACH_QUORUM;
        tracker.enableDCTracking(replicaPlan);
    }

    /**
     * We need to have received the quorum majority count in each DC to consider the query successful
     */
    @Override
    public boolean receivedSufficientResponses()
    {
        return tracker.hitDCConsistencyLevel();
    }
}