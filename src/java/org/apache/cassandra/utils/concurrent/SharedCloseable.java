/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

/**
 * A simple extension of AutoCloseable, that represents a resource that can be utilised in multiple locations,
 * each managing their own closure of the resource, so that when the last such instance is closed all are.
 *
 */
public interface SharedCloseable extends AutoCloseable
{
    /**
     * @return a new instance of the object representing the same state and backed by the same underlying resources.
     * Coordinates with the original (and other instances) when the underlying resource should be closed.
     * Throws an exception if the shared resource has already been closed.
     */
    public SharedCloseable sharedCopy();
    public Throwable close(Throwable accumulate);

    public void addTo(Ref.IdentityCollection identities);
}
