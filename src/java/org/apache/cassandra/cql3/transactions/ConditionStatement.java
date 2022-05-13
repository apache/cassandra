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

package org.apache.cassandra.cql3.transactions;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.service.accord.txn.TxnCondition;

public class ConditionStatement
{
    public enum Kind
    {
        IS_NOT_NULL(TxnCondition.Kind.IS_NOT_NULL, null),
        IS_NULL(TxnCondition.Kind.IS_NULL, null),
        EQ(TxnCondition.Kind.EQUAL, TxnCondition.Kind.EQUAL),
        NEQ(TxnCondition.Kind.NOT_EQUAL, TxnCondition.Kind.NOT_EQUAL),
        GT(TxnCondition.Kind.GREATER_THAN, TxnCondition.Kind.LESS_THAN),
        GTE(TxnCondition.Kind.GREATER_THAN_OR_EQUAL, TxnCondition.Kind.LESS_THAN_OR_EQUAL),
        LT(TxnCondition.Kind.LESS_THAN, TxnCondition.Kind.GREATER_THAN),
        LTE(TxnCondition.Kind.LESS_THAN_OR_EQUAL, TxnCondition.Kind.GREATER_THAN_OR_EQUAL);
        
        // TODO: Support for IN, CONTAINS, CONTAINS KEY

        private final TxnCondition.Kind kind;
        private final TxnCondition.Kind reversedKind;
        
        Kind(TxnCondition.Kind kind, TxnCondition.Kind reversedKind)
        {
            this.kind = kind;
            this.reversedKind = reversedKind;
        }

        TxnCondition.Kind toTxnKind(boolean reversed)
        {
            return reversed ? reversedKind : kind;
        }
    }

    private final RowDataReference reference;
    private final Kind kind;
    private final Term value;
    private final boolean reversed;

    public ConditionStatement(RowDataReference reference, Kind kind, Term value, boolean reversed)
    {
        this.reference = reference;
        this.kind = kind;
        this.value = value;
        this.reversed = reversed;
    }

    public static class Raw
    {
        private final Term.Raw lhs;
        private final Kind kind;
        private final Term.Raw rhs;

        public Raw(Term.Raw lhs, Kind kind, Term.Raw rhs)
        {
            Preconditions.checkArgument(lhs != null);
            Preconditions.checkArgument((rhs == null) == (kind == Kind.IS_NOT_NULL || kind == Kind.IS_NULL));
            this.lhs = lhs;
            this.kind = kind;
            this.rhs = rhs;
        }

        public ConditionStatement prepare(String keyspace, VariableSpecifications bindVariables)
        {
            if (rhs == null)
            {
                // In the IS NULL/IS NOT NULL case, the reference will always be on the LHS
                RowDataReference reference = ((RowDataReference.Raw) lhs).prepareAsReceiver();
                reference.collectMarkerSpecification(bindVariables);
                return new ConditionStatement(reference, kind, null, false);
            }
                
            RowDataReference reference;
            Term value;
            boolean reversed = false;
            
            if (lhs instanceof RowDataReference.Raw)
            {
                reference = ((RowDataReference.Raw) lhs).prepareAsReceiver();
                ColumnSpecification receiver = reference.getValueReceiver();
                value = rhs.prepare(keyspace, receiver);
            }
            else if (rhs instanceof RowDataReference.Raw)
            {
                reference = ((RowDataReference.Raw) rhs).prepareAsReceiver();
                ColumnSpecification receiver = reference.getValueReceiver();
                value = lhs.prepare(keyspace, receiver);
                // TxnCondition expects the reference to be on the LHS, so reverse the operator.
                reversed = true;
            }
            else
            {
                throw new IllegalStateException("Either the left-hand or right-hand side must be a reference!");
            }

            reference.collectMarkerSpecification(bindVariables);
            value.collectMarkerSpecification(bindVariables);
            return new ConditionStatement(reference, kind, value, reversed);
        }
    }

    public TxnCondition createCondition(QueryOptions options)
    {
        switch (kind)
        {
            case IS_NOT_NULL:
            case IS_NULL:
                return new TxnCondition.Exists(reference.toTxnReference(options), kind.toTxnKind(reversed));
            case EQ:
            case NEQ:
            case GT:
            case GTE:
            case LT:
            case LTE:
                // TODO: Support for references on LHS and RHS
                return new TxnCondition.Value(reference.toTxnReference(options),
                                              kind.toTxnKind(reversed),
                                              value.bindAndGet(options),
                                              options.getProtocolVersion());
            default:
                throw new IllegalStateException();
        }
    }
}
