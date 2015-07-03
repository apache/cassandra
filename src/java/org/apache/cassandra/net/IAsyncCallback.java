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
package org.apache.cassandra.net;

import java.net.InetAddress;

import com.google.common.base.Predicate;

import org.apache.cassandra.gms.FailureDetector;

/**
 * implementors of IAsyncCallback need to make sure that any public methods
 * are threadsafe with respect to response() being called from the message
 * service.  In particular, if any shared state is referenced, making
 * response alone synchronized will not suffice.
 */
public interface IAsyncCallback<T>
{
    Predicate<InetAddress> isAlive = new Predicate<InetAddress>()
    {
        public boolean apply(InetAddress endpoint)
        {
            return FailureDetector.instance.isAlive(endpoint);
        }
    };

    /**
     * @param msg response received.
     */
    void response(MessageIn<T> msg);

    /**
     * @return true if this callback is on the read path and its latency should be
     * given as input to the dynamic snitch.
     */
    boolean isLatencyForSnitch();
}
