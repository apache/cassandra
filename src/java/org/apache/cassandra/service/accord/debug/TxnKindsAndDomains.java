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

import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;

public class TxnKindsAndDomains
{
    static final int ALL_KINDS = Txn.Kind.All.bitset();
    public static final TxnKindsAndDomains ALL = new TxnKindsAndDomains(false, ALL_KINDS, ALL_KINDS);
    public static final TxnKindsAndDomains NONE = new TxnKindsAndDomains(false, 0, 0);

    final boolean printAsSubtraction;
    final int keys, ranges;

    public TxnKindsAndDomains(boolean printAsSubtraction, int keys, int ranges)
    {
        this.printAsSubtraction = printAsSubtraction;
        this.keys = keys;
        this.ranges = ranges;
    }

    public boolean matches(TxnId txnId)
    {
        int bits = txnId.is(Domain.Key) ? keys : ranges;
        return TinyEnumSet.contains(bits, txnId.kind());
    }

    public boolean matchesAny(Domain domain)
    {
        switch (domain)
        {
            default: throw new UnhandledEnum(domain);
            case Key: return keys != 0;
            case Range: return ranges != 0;
        }
    }

    @Override
    public String toString()
    {
        if (keys == ALL_KINDS && ranges == ALL_KINDS)
            return "*";
        if (printAsSubtraction)
            return '-' + toString(ALL_KINDS & ~keys, ALL_KINDS & ~ranges);
        return '+' + toString(keys, ranges);
    }

    public static TxnKindsAndDomains parse(String input)
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
            throw new IllegalArgumentException("Invalid TxnKindsAndDomain specification: " + input);

        int keys = 0, ranges = 0;
        for (String element : input.substring(1, input.length() - 1).split("\\s*,\\s*"))
        {
            if (element.length() != 2)
                throw new IllegalArgumentException("Invalid TxnKindsAndDomain element: " + element);

            int kinds;
            if (element.charAt(1) == '*') kinds = ALL_KINDS;
            else
            {
                Txn.Kind kind = Txn.Kind.forShortName(element.charAt(1));
                if (kind == null) throw new IllegalArgumentException("Unknown Txn.Kind: " + element.charAt(1));
                kinds = TinyEnumSet.encode(kind);
            }

            switch (element.charAt(0))
            {
                default:
                    throw new IllegalArgumentException("Invalid TxnKindsAndDomain element: " + element);
                case '*':
                    keys |= kinds;
                    ranges |= kinds;
                    break;
                case 'K':
                    keys |= kinds;
                    break;
                case 'R':
                    ranges |= kinds;
                    break;
            }
        }

        if (subtraction)
        {
            keys = ALL_KINDS & ~keys;
            ranges = ALL_KINDS & ~ranges;
        }
        return new TxnKindsAndDomains(subtraction, keys, ranges);
    }

    private static String toString(int keys, int ranges)
    {
        StringBuilder out = new StringBuilder("{");
        if (keys != 0)
        {
            if (keys == ALL_KINDS) out.append("K*");
            else TinyEnumSet.append(keys, Txn.Kind::forOrdinal, k -> "K" + k.shortName(), out);
        }

        if (ranges != 0)
        {
            if (keys != 0) out.append(',');
            if (ranges == ALL_KINDS) out.append("R*");
            else TinyEnumSet.append(ranges, Txn.Kind::forOrdinal, k -> "R" + k.shortName(), out);
        }
        out.append('}');
        return out.toString();
    }
}
