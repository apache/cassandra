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
package org.apache.cassandra.gms;

public enum ApplicationState
{
    STATUS,
    LOAD,
    SCHEMA,
    DC,
    RACK,
    RELEASE_VERSION,
    REMOVAL_COORDINATOR,
    INTERNAL_IP,
    RPC_ADDRESS,
    X_11_PADDING, // padding specifically for 1.1
    SEVERITY,
    NET_VERSION,
    HOST_ID,
    TOKENS,
    RPC_READY,
    // pad to allow adding new states to existing cluster
    X1,
    X2,
    X3,
    X4,
    X5,
    X6,
    X7,
    X8,
    X9,
    X10,
}
