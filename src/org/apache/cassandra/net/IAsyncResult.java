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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IAsyncResult
{    
    /**
     * This is used to check if the task has been completed
     * 
     * @return true if the task has been completed and false otherwise.
     */
    public boolean isDone();
    
    /**
     * Returns the result for the task that was submitted.
     * @return the result wrapped in an Object[]
    */
    public Object[] get();    
    
    /**
     * Same operation as the above get() but allows the calling
     * thread to specify a timeout.
     * @param timeout the maximum time to wait
     * @param tu the time unit of the timeout argument
     * @return the result wrapped in an Object[]
    */
    public Object[] get(long timeout, TimeUnit tu) throws TimeoutException;
    
    /**
     * Returns the result for all tasks that was submitted.
     * @return the list of results wrapped in an Object[]
    */
    public List<Object[]> multiget();
    
    /**
     * Same operation as the above get() but allows the calling
     * thread to specify a timeout.
     * @param timeout the maximum time to wait
     * @param tu the time unit of the timeout argument
     * @return the result wrapped in an Object[]
    */
    public List<Object[]> multiget(long timeout, TimeUnit tu) throws TimeoutException;
    
    /**
     * Store the result obtained for the submitted task.
     * @param result result wrapped in an Object[]
     * 
     * @param result the response message
     */
    public void result(Message result);
}
