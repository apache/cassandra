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


public interface IMeasurableMemory
{
    /**
     * @return the amount of on-heap memory retained by the object that might be reclaimed if the object were reclaimed,
     * i.e. it should try to exclude globally cached data where possible, or counting portions of arrays that are
     * referenced by the object but used by other objects only (e.g. slabbed byte-buffers), etc.
     */
    public long unsharedHeapSize();
}
