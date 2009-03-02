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

package org.apache.cassandra.gms;

/**
 * This is implemented by the Gossiper module to publish change events to interested parties.
 * Interested parties register/unregister interest by invoking the methods of this interface.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IEndPointStateChangePublisher
{
    /**
     * Register for interesting state changes.
     * @param subcriber module which implements the IEndPointStateChangeSubscriber
     */
    public void register(IEndPointStateChangeSubscriber subcriber);
    
    /**
     * Unregister interest for state changes.
     * @param subcriber module which implements the IEndPointStateChangeSubscriber
     */
    public void unregister(IEndPointStateChangeSubscriber subcriber);
}
