package org.apache.cassandra.cache;
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


public interface InstrumentingCacheMBean
{
    public int getCapacity();
    public void setCapacity(int capacity);
    public int getSize();

    /** total request count since cache creation */
    public long getRequests();

    /** total cache hit count since cache creation */
    public long getHits();

    /**
     * hits / requests since the last time getHitRate was called.  serious telemetry apps should not use this,
     * and should instead track the deltas from getHits / getRequests themselves, since those will not be
     * affected by multiple users calling it.  Provided for convenience only.
     */
    public double getRecentHitRate();
}
