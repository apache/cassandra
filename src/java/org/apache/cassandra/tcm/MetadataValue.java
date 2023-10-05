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

package org.apache.cassandra.tcm;

/**
 * A Cluster metadata element.
 * @param <V> the {@code MetadataValue} object type.
 */
public interface MetadataValue<V>
{
    /**
     * Creates a copy of this {@code MetadataValue} instance with the specified epoch.
     * @param epoch the new epoch
     * @return a copy of this {@code MetadataValue} instance with the specified epoch
     */
    V withLastModified(Epoch epoch);

    /**
     * Returns the epoch at which this value was last modified.
     * @return the epoch at which this value was last modified.
     */
    Epoch lastModified();
}
