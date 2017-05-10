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

package org.apache.cassandra.service.throttler;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestThrottledException;

/**
 * Interface which defines a policy to throttle read and write requests.
 */
public interface IRequestThrottler
{
    /**
     * Integer that is returned in the Exception that is thrown if the request is throttled.
     * A reference to HTTP 420: Enhance your calm.
     */
    public static final int REQUEST_THROTTLE_ERROR_INT = -420;

    /**
     * Called once in the lifetime of the daemon to setup the request_throttler.
     * This is called after the node finishes joining the ring.
     */
    public void setup();

    /**
     * Called every time a read command is issued by the service.
     * The Throttler can inspect if {code consistencyLevel.isSerial()} to distinguish between
     * light weight transactions and normal reads.
     *
     * The Throttler can inspect whether:
     *     {code command instanceof SinglePartitionReadCommand} or
     *     {code command instanceof PartitionRangeReadCommand}
     * to distinguish between single or range partition read commands.
     *
     * @param command The read command that is issued by the service.
     * @param consistencyLevel The consistency level of the read command.
     * @throws RequestThrottledException iff this read request should be throttled.
     */
    public void maybeThrottleRead(ReadCommand command, ConsistencyLevel consistencyLevel) throws RequestThrottledException;

    /**
     * Called every time a mutation is issued by the service.
     * The Throttler can inspect if {code consistencyLevel.isSerial()} to distinguish between
     * Light weight transcations and normal mutations.
     *
     * @param mutation The mutation that is issued by the service.
     * @param consistencyLevel The consistency level of the mutation.
     * @throws RequestThrottledException iff this mutation request should be throttled.
     */
    public void maybeThrottleMutation(IMutation mutation, ConsistencyLevel consistencyLevel) throws RequestThrottledException;

}
