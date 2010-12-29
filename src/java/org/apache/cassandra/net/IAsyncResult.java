/**
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface IAsyncResult extends IMessageCallback
{    
    /**
     * Same operation as the above get() but allows the calling
     * thread to specify a timeout.
     * @param timeout the maximum time to wait
     * @param tu the time unit of the timeout argument
     * @return the result wrapped in an Object[]
    */
    public byte[] get(long timeout, TimeUnit tu) throws TimeoutException;
        
    /**
     * Store the result obtained for the submitted task.
     * @param result the response message
     */
    public void result(Message result);

    public InetAddress getFrom();
}
