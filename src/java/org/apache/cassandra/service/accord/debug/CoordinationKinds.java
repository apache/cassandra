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

package org.apache.cassandra.service.accord.debug;

import accord.coordinate.Coordination;
import accord.utils.TinyEnumSet;

public class CoordinationKinds extends TinyEnumSet<Coordination.CoordinationKind>
{
    private static final int ALL_BITS = Coordination.CoordinationKind.ALL.bitset();
    public static final CoordinationKinds ALL = new CoordinationKinds(false, ALL_BITS);
    public static final CoordinationKinds NONE = new CoordinationKinds(false, 0);

    final boolean printAsSubtraction;

    public CoordinationKinds(boolean printAsSubtraction, int bitset)
    {
        super(bitset);
        this.printAsSubtraction = printAsSubtraction;
    }

    @Override
    public String toString()
    {
        if (bitset == ALL_BITS)
            return "*";
        if (printAsSubtraction)
            return '-' + toString(ALL_BITS & ~bitset);
        return toString(bitset, Coordination.CoordinationKind::forOrdinal);
    }

    public static CoordinationKinds parse(String input)
    {
        input = input.trim();
        if (input.equals("*"))
            return ALL;
        if (input.equals("{}"))
            return NONE;

        boolean subtraction = false;
        if (input.length() >= 1 && input.charAt(0) == '-')
        {
            subtraction = true;
            input = input.substring(1);
        }
        if (input.length() < 2 || input.charAt(0) != '{' || input.charAt(input.length() - 1) != '}')
            throw new IllegalArgumentException("Invalid CoordinationKinds specification: " + input);

        int bits = 0;
        for (String name : input.substring(1, input.length() - 1).split("\\s*,\\s*"))
            bits |= TinyEnumSet.encode(Coordination.CoordinationKind.valueOf(name));

        if (subtraction)
            bits = ALL_BITS & ~bits;
        return new CoordinationKinds(subtraction, bits);
    }

    private static String toString(int bitset)
    {
        return TinyEnumSet.toString(bitset, Coordination.CoordinationKind::forOrdinal);
    }
}
