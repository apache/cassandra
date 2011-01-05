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

package org.apache.cassandra.cql.driver;

public interface IConnectionPool
{
    /**
     * Check a <code>Connection</code> instance out from the pool, creating a
     * new one if the pool is exhausted.
     */
    public Connection borrowConnection();
    
    /**
     * Returns an <code>Connection</code> instance to the pool.  If the pool
     * already contains the maximum number of allowed connections, then the
     * instance's <code>close</code> method is called and it is discarded.
     */
    public void returnConnection(Connection connection);
}
