package org.apache.cassandra.scheduler;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.util.concurrent.TimeoutException;

/**
 * Implementors of IRequestScheduler must provide a constructor taking a RequestSchedulerOptions object.
 */
public interface IRequestScheduler
{
    /**
     * Queue incoming request threads
     * 
     * @param t Thread handing the request
     * @param id    Scheduling parameter, an id to distinguish profiles (users/keyspace)
     * @param timeout   The max time in milliseconds to spend blocking for a slot
     */
    public void queue(Thread t, String id, long timeoutMS) throws TimeoutException;

    /**
     * A convenience method for indicating when a particular request has completed
     * processing, and before a return to the client
     */
    public void release();
}
