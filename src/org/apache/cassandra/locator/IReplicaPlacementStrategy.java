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

package org.apache.cassandra.locator;

import java.math.BigInteger;
import java.util.Map;

import org.apache.cassandra.net.EndPoint;


/*
 * This interface has two implementations. One which
 * does not respect rack or datacenter awareness and
 * the other which does.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public interface IReplicaPlacementStrategy
{
	public EndPoint[] getStorageEndPoints(BigInteger token);
    public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap);
    public Map<EndPoint, EndPoint> getHintedStorageEndPoints(BigInteger token);    
}
