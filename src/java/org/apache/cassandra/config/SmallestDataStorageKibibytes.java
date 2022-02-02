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

package org.apache.cassandra.config;

/**
 * Wrapper class for Cassandra data storage configuration parameters which are internally represented in Kibibytes. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only kibibytes or larger units. (CASSANDRA-15234)
 */
public class SmallestDataStorageKibibytes extends DataStorageSpec
{
    /**
     * Creates a {@code SmallestDataStorageKibibytes} of the specified amount of seconds and provides the smallest
     * required unit of Kibibytes for the respective parameter of type {@code SmallestDurationSeconds}.
     *
     * @param value the data storage
     *
     */
    public SmallestDataStorageKibibytes(String value)
    {
        super(value, DataStorageSpec.DataStorageUnit.KIBIBYTES);
    }

    private SmallestDataStorageKibibytes(long quantity, DataStorageSpec.DataStorageUnit unit)
    {
        super(quantity, unit);
    }

    /**
     * Creates a {@code SmallestDataStorageKibibytes} of the specified amount of kibibytes.
     *
     * @param kibibytes the amount of kibibytes
     * @return a data storage
     */
    public static SmallestDataStorageKibibytes inKibibytes(long kibibytes)
    {
        return new SmallestDataStorageKibibytes(kibibytes, DataStorageSpec.DataStorageUnit.KIBIBYTES);
    }
}
