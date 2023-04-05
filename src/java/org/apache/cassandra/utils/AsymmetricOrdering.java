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
package org.apache.cassandra.utils;

import java.util.List;

import com.google.common.collect.Ordering;

import net.nicoulaj.compilecommand.annotations.Inline;

public abstract class AsymmetricOrdering<T1, T2> extends Ordering<T1>
{

    public abstract int compareAsymmetric(T1 left, T2 right);

    public static enum Op
    {
        // maximum index < key; -1 if no such key. == CEIL - 1
        LOWER,

        // maximum index <= key; -1 if no such key. == HIGHER + 1
        FLOOR,

        // minimum index >= key; size() if no such key.  == LOWER + 1
        CEIL,

        // minimum index > key; size() if no such key. == FLOOR - 1
        HIGHER
    }

    /**
     * @param searchIn sorted list to look in
     * @param searchFor key to find
     */
    public int binarySearchAsymmetric(List<? extends T1> searchIn, T2 searchFor, Op op)
    {
        final int strictnessOfLessThan = strictnessOfLessThan(op);
        int lb = -1;
        int ub = searchIn.size();
        // a[-1]            ^= -infinity
        // a[search.size()] ^= +infinity

        while (lb + 1 < ub)
        {
            int m = (lb + ub) / 2;
            int c = compareAsymmetric(searchIn.get(m), searchFor);

            if (c < strictnessOfLessThan) lb = m;
            else ub = m;
        }

        return selectBoundary(op, lb, ub);
    }

    @Inline
    // this value, used as the right operand to a less than operator for the result
    // of a compare() makes its behaviour either strict (<) or not strict (<=).
    // a value of 1 is not strict, whereas 0 is strict
    private static int strictnessOfLessThan(Op op)
    {
        switch (op)
        {
            case FLOOR: case HIGHER:

            // { a[lb] <= v ^ a[ub] > v }
            return 1;

            // { a[m] >  v   ==>   a[ub] >  v   ==>   a[lb] <= v ^ a[ub] > v }
            // { a[m] <= v   ==>   a[lb] <= v   ==>   a[lb] <= v ^ a[ub] > v }

            case CEIL: case LOWER:

            // { a[lb] < v ^ a[ub] >= v }

            return 0;

            // { a[m] >= v   ==>   a[ub] >= v   ==>   a[lb] < v ^ a[ub] >= v }
            // { a[m] <  v   ==>   a[lb] <  v   ==>   a[lb] < v ^ a[ub] >= v }
        }
        throw new IllegalStateException();
    }

    @Inline
    private static int selectBoundary(Op op, int lb, int ub)
    {
        switch (op)
        {
            case CEIL:
                // { a[lb] < v ^ a[ub] >= v }
            case HIGHER:
                // { a[lb] <= v ^ a[ub] > v }
                return ub;
            case FLOOR:
                // { a[lb] <= v ^ a[ub] > v }
            case LOWER:
                // { a[lb] < v ^ a[ub] >= v }
                return lb;
        }
        throw new IllegalStateException();
    }

    private class Reversed extends AsymmetricOrdering<T1, T2>
    {
        public int compareAsymmetric(T1 left, T2 right)
        {
            return -AsymmetricOrdering.this.compareAsymmetric(left, right);
        }

        public int compare(T1 left, T1 right)
        {
            return AsymmetricOrdering.this.compare(right, left);
        }

        public AsymmetricOrdering<T1, T2> reverse()
        {
            return AsymmetricOrdering.this;
        }
    }

    public AsymmetricOrdering<T1, T2> reverse()
    {
        return new Reversed();
    }
}
