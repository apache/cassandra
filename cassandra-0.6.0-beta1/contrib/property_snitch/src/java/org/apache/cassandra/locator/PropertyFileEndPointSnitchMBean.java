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

package src.java.org.apache.cassandra.locator;

import java.io.IOException;

/**
 * PropertyFileEndPointSnitchMBean
 * 
 * PropertyFileEndPointSnitchMBean is the management interface for Digg's EndpointSnitch MBean
 * 
 * @author Sammy Yu <syu@sammyyu.net>
 * 
 */
public interface PropertyFileEndPointSnitchMBean {
    /**
     * The object name of the mbean.
     */
    public static String MBEAN_OBJECT_NAME = "org.apache.cassandra.locator:type=EndPointSnitch";
    
    /**
     * Reload the rack configuration
     */
    public void reloadConfiguration() throws IOException;
    
    /**
     * Display the current configuration
     */
    public String displayConfiguration();

}
