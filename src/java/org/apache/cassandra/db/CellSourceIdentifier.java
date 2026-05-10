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

package org.apache.cassandra.db;

/**
 * A cell's source data object. Can be used to determine if two cells originated from the same object, e.g. memtable
 * or sstable.
 */
public interface CellSourceIdentifier
{
    /**
     * Returns true iff this and other CellSourceIdentifier are equal, indicating that the cell are from the same
     * source.
     * @param other the other source with which to compare
     * @return true if the two sources are equal
     */
    default boolean isEqualSource(CellSourceIdentifier other)
    {
        return this.equals(other);
    }
}
