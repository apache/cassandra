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

package org.apache.cassandra.db;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * A clock used for conflict resolution.
 */
public interface IClock
{
    public static enum ClockRelationship
    {
        EQUAL,
        GREATER_THAN,
        LESS_THAN,
        DISJOINT
    };

    /**
     * @param other Compare these two clocks.
     * @return The relationship between the two clocks,
     * lets us know if reconciliation will have to take place.
     */
    public ClockRelationship compare(IClock other);

    /**
     * @param otherClocks The other clock to use when extracting the superset.
     * @return The superset of the two clocks.
     */
    public IClock getSuperset(List<IClock> otherClocks);

    /**
     * @return number of bytes this type of clock
     * uses up when serialized.
     */
    public int size();

    /**
     * @return the type of this clock.
     */
    public ClockType type();

    /**
     * @param out Write a serialized representation of this clock to the output.
     * @throws IOException Thrown if writing failed.
     */
    public void serialize(DataOutput out) throws IOException;

    /**
     * @return a textual representation of this clock.
     */
    public String toString();
}
