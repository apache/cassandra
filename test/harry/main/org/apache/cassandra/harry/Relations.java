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

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.gen.Bijections;

public class Relations
{
    private static final Logger logger = LoggerFactory.getLogger(Relations.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static boolean matchRange(Bijections.Bijection<Object[]> ckGen,
                                     IntFunction<Comparator<Object>> comparators,
                                     int ckCoulmnCount,
                                     long lowBoundDescr, long highBoundDescr,
                                     Relations.RelationKind[] lowBoundRelations, Relations.RelationKind[] highBoundRelations,
                                     long matchDescr)
    {
        Object[] lowBoundValue = lowBoundDescr == MagicConstants.UNSET_DESCR ? null : ckGen.inflate(lowBoundDescr);
        Object[] highBoundValue = highBoundDescr == MagicConstants.UNSET_DESCR ? null : ckGen.inflate(highBoundDescr);
        Object[] matchValue = ckGen.inflate(matchDescr);
        // TODO: assert that all equals + null checks
        for (int i = 0; i < ckCoulmnCount; i++)
        {
            Object matched = matchValue[i];

            if (lowBoundValue != null)
            {
                Object l = lowBoundValue[i];
                Relations.RelationKind lr = lowBoundRelations[i];

                if (lr != null && !lr.match(comparators.apply(i), matched, l))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Low Bound {} {} {} did match {}", lowBoundValue[i], lr, matchValue[i], i);
                    return false;
                }
            }

            if (highBoundValue != null)
            {
                Object h = highBoundValue[i];
                Relations.RelationKind hr = highBoundRelations[i];

                if (hr != null && !hr.match(comparators.apply(i), matched, h))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("High Bound {} {} {} did match {}", highBoundValue[i], hr, matchValue[i], i);
                    return false;
                }
            }
        }

        if (logger.isTraceEnabled())
            logger.trace("{} is between {} and {} fully matched match", Arrays.toString(matchValue), Arrays.toString(lowBoundValue), Arrays.toString(highBoundValue));
        return true;
    }

    public static class Relation
    {
        public final Relations.RelationKind kind;
        public final long descriptor;
        public final int column;
        public Relation(RelationKind kind, long descriptor, int column)
        {
            this.kind = kind;
            this.descriptor = descriptor;
            this.column = column;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public enum RelationKind
    {
        LT
        {
            @Override
            public boolean match(Comparator<Object> comparator, Object l, Object r)
            {
                return comparator.compare(l, r) < 0;
            }

            @Override
            public String symbol()
            {
                return "<";
            }

            @Override
            public RelationKind reverse()
            {
                return GT;
            }
        },
        GT
        {
            @Override
            public boolean match(Comparator<Object> comparator, Object l, Object r)
            {
                return comparator.compare(l, r) > 0;
            }

            @Override
            public String symbol()
            {
                return ">";
            }

            @Override
            public RelationKind reverse()
            {
                return LT;
            }
        },
        LTE
        {
            @Override
            public boolean match(Comparator<Object> comparator, Object l, Object r)
            {
                return comparator.compare(l, r) <= 0;
            }

            @Override
            public String symbol()
            {
                return "<=";
            }

            @Override
            public RelationKind reverse()
            {
                return GTE;
            }
        },
        GTE
        {
            @Override
            public boolean match(Comparator<Object> comparator, Object l, Object r)
            {
                return comparator.compare(l, r) >= 0;
            }

            @Override
            public String symbol()
            {
                return ">=";
            }

            @Override
            public RelationKind reverse()
            {
                return LTE;
            }
        },
        EQ
        {
            @Override
            public boolean match(Comparator<Object> comparator, Object l, Object r)
            {
                return comparator.compare(l, r) == 0;
            }

            @Override
            public String symbol()
            {
                return "=";
            }

            @Override
            public RelationKind reverse()
            {
                return EQ;
            }
        };

        public abstract boolean match(Comparator<Object> comparator, Object l, Object r);

        public abstract String symbol();

        public abstract RelationKind reverse();
    }
}