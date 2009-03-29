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

package org.apache.cassandra.service;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.cassandra.net.EndPoint;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface StorageServiceMBean
{    
    public String getLiveNodes();
    public String getUnreachableNodes();
    public String getToken();
    
    /**
     * This method will cause the local node initiate
     * the bootstrap process for all the nodes specified
     * in the string parameter passed in. This local node
     * will calculate who gives what ranges to the nodes
     * and then instructs the nodes to do so.
     * 
     * @param nodes colon delimited list of endpoints that need
     *              to be bootstrapped
    */
    public void loadAll(String nodes);
    
    /**
     * This method is used only for debug purpose.  
    */
    public void updateToken(String token);    
    
    /**
     * 
     */
    public void doGC();

    /**
     * Stream the files in the bootstrap directory over to the
     * node being bootstrapped. This is used in case of normal
     * bootstrap failure. Use a tool to re-calculate the cardinality
     * at a later point at the destination.
     * @param sources colon separated list of directories from where 
     *                files need to be picked up.
     * @param target endpoint receiving data.
    */
    public void forceHandoff(String directories, String target) throws IOException;
}
