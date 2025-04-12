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

package org.apache.cassandra.harry;

import java.util.Set;

import org.apache.cassandra.harry.util.BitSet;

public class MagicConstants
{
    public static final BitSet ALL_COLUMNS = BitSet.allSet(64);
    /**
     * For keys
     */
    public static final Object[] UNKNOWN_KEY = new Object[]{};
    public static final Object[] NIL_KEY = new Object[]{};

    /**
     * For values
     */
    public static final Object UNKNOWN_VALUE = new Object() {
        public String toString()
        {
            return "UNKNOWN";
        }
    };
    public static final Object UNSET_VALUE = new Object() {
        public String toString()
        {
            return "UNSET";
        }
    };
    /**
     * For Descriptors
     */
    public static final long UNKNOWN_DESCR = Long.MIN_VALUE + 2;
    // TODO: Empty value, for the types that support it
    public static final long EMPTY_VALUE_DESCR = Long.MIN_VALUE + 1;
    public static final long UNSET_DESCR = Long.MIN_VALUE + 3;
    public static final long NIL_DESCR = Long.MIN_VALUE;
    public static final Set<Long> MAGIC_DESCRIPTOR_VALS = Set.of(UNKNOWN_DESCR, EMPTY_VALUE_DESCR, UNSET_DESCR, NIL_DESCR);
    /**
     * For LTS
     */
    public static final long[] LTS_UNKNOWN = new long[]{};
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    /**
     * For indices
     */
    public static final int UNKNOWN_IDX = Integer.MIN_VALUE + 2;
    public static final int UNSET_IDX = Integer.MIN_VALUE + 1;
    public static final int NIL_IDX = Integer.MIN_VALUE;
}
