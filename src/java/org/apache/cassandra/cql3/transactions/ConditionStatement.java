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
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.accord.txn.TxnCondition;

public class ConditionStatement
{
    public enum Kind
    {
        IS_NOT_NULL(TxnCondition.Kind.IS_NOT_NULL),
        IS_NULL(TxnCondition.Kind.IS_NULL),
        EQ(TxnCondition.Kind.EQUAL),
        NEQ(TxnCondition.Kind.NOT_EQUAL),
        GT(TxnCondition.Kind.GREATER_THAN),
        GTE(TxnCondition.Kind.GREATER_THAN_OR_EQUAL),
        LT(TxnCondition.Kind.LESS_THAN),
        LTE(TxnCondition.Kind.LESS_THAN_OR_EQUAL);
        
        // TODO: Support for IN, CONTAINS, CONTAINS KEY

        private final TxnCondition.Kind kind;
        
        Kind(TxnCondition.Kind kind)
        {
            this.kind = kind;
        }

        TxnCondition.Kind toTxnKind()
        {
            return kind;
        }
    }

    private final RowDataReference reference;
    private final Kind kind;
    private final Term value;

    public ConditionStatement(RowDataReference reference, Kind kind, Term value)
    {
        this.reference = reference;
        this.kind = kind;
        this.value = value;
    }

    public static class Raw
    {
        private final RowDataReference.Raw reference;
        private final Kind kind;
        private final Term.Raw value;

        public Raw(RowDataReference.Raw reference, Kind kind, Term.Raw value)
        {
            Preconditions.checkArgument(reference != null);
            Preconditions.checkArgument((value == null) == (kind == Kind.IS_NOT_NULL || kind == Kind.IS_NULL));
            this.reference = reference;
            this.kind = kind;
            this.value = value;
        }

        public ConditionStatement prepare(String keyspace, VariableSpecifications bindVariables)
        {
            RowDataReference preparedReference = reference.prepareAsReceiver();
            preparedReference.collectMarkerSpecification(bindVariables);
            Term preparedValue = null;

            if (value != null)
            {
                ColumnSpecification receiver = preparedReference.column();
                
                if (preparedReference.isElementSelection())
                {
                    switch (((CollectionType<?>) receiver.type).kind)
                    {
                        case LIST:
                            receiver = Lists.valueSpecOf(receiver);
                            break;
                        case MAP:
                            receiver = Maps.valueSpecOf(receiver);
                            break;
                        case SET:
                            throw new InvalidRequestException(String.format("Invalid operation %s = %s for set column %s",
                                                                            preparedReference.getFullyQualifiedName(), value, receiver.name));
                    }
                }
                else if (preparedReference.isFieldSelection())
                {
                    receiver = preparedReference.getFieldSelectionSpec();
                }

                preparedValue = value.prepare(keyspace, receiver);
                preparedValue.collectMarkerSpecification(bindVariables);
            }

            return new ConditionStatement(preparedReference, kind, preparedValue);
        }
    }

    public TxnCondition createCondition(QueryOptions options)
    {
        switch (kind)
        {
            case IS_NOT_NULL:
            case IS_NULL:
                return new TxnCondition.Exists(reference.toTxnReference(options), kind.toTxnKind());
            case EQ:
            case NEQ:
            case GT:
            case GTE:
            case LT:
            case LTE:
                return new TxnCondition.Value(reference.toTxnReference(options), kind.toTxnKind(), value.bindAndGet(options));
            default:
                throw new IllegalStateException();
        }
    }
}
