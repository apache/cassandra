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
package org.apache.cassandra.cache;

public interface CacheSize
{

    /**
     * Returns the maximum total weighted or unweighted size (number of entries) of this cache, depending on how the
     * cache was constructed.
     */
    long capacity();

    /**
     * Specifies the maximum total size of this cache. This value may be interpreted as the weighted or unweighted
     * (number of entries) threshold size based on how this cache was constructed.
     */
    void setCapacity(long capacity);

    /**
     * Returns the approximate number of entries in this cache.
     */
    int size();

    /**
     * Returns the approximate accumulated weight of entries in this cache. If this cache does not use a weighted size
     * bound, then it equals to the approximate number of entries.
     */
    long weightedSize();

}
