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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A restriction/clause on a column.
 * The goal of this class being to group all conditions for a column in a SELECT.
 */
public interface Restriction
{
    public boolean isOnToken();

    public boolean isSlice();
    public boolean isEQ();
    public boolean isIN();
    public boolean isContains();
    public boolean isMultiColumn();

    /**
     * Returns true if, when applied to a clustering column, this restriction can be handled through one or more slices
     * alone without filtering.  For example, EQ restrictions can be represented as a slice, but CONTAINS cannot.
     */
    public boolean canEvaluateWithSlices();

    // Not supported by Slice, but it's convenient to have here
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException;

    public static interface EQ extends Restriction {}

    public static interface IN extends Restriction
    {
        public boolean canHaveOnlyOneValue();
    }

    public static interface Slice extends Restriction
    {
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException;

        /** Returns true if the start or end bound (depending on the argument) is set, false otherwise */
        public boolean hasBound(Bound b);

        public ByteBuffer bound(Bound b, QueryOptions options) throws InvalidRequestException;

        /** Returns true if the start or end bound (depending on the argument) is inclusive, false otherwise */
        public boolean isInclusive(Bound b);

        public Operator getRelation(Bound eocBound, Bound inclusiveBound);

        public Operator getIndexOperator(Bound b);

        public void setBound(Operator type, Term t) throws InvalidRequestException;
    }
}
