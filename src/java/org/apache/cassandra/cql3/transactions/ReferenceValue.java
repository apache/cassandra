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

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.txn.TxnReferenceValue;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

public abstract class ReferenceValue
{
    public abstract TxnReferenceValue bindAndGet(QueryOptions options);

    public static abstract class Raw extends Term.Raw
    {
        public abstract ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables);
    }

    public static class Constant extends ReferenceValue
    {
        private final Term term;

        public Constant(Term term)
        {
            this.term = term;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Constant(term.bindAndGet(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final Term.Raw term;

            public Raw(Term.Raw term)
            {
                this.term = term;
            }

            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                return new Constant(term.prepare(receiver.ksName, receiver));
            }

            @Override
            public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
            {
                return term.testAssignment(keyspace, receiver);
            }

            @Override
            public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
            {
                return term.prepare(keyspace, receiver);
            }

            @Override
            public String getText()
            {
                return term.getText();
            }

            @Override
            public AbstractType<?> getExactTypeIfKnown(String keyspace)
            {
                return term.getExactTypeIfKnown(keyspace);
            }
        }
    }

    public static class Substitution extends ReferenceValue
    {
        private final RowDataReference reference;

        public Substitution(RowDataReference reference)
        {
            this.reference = reference;
        }

        @Override
        public TxnReferenceValue bindAndGet(QueryOptions options)
        {
            return new TxnReferenceValue.Substitution(reference.toTxnReference(options));
        }

        public static class Raw extends ReferenceValue.Raw
        {
            private final RowDataReference.Raw reference;

            public Raw(RowDataReference.Raw reference)
            {
                this.reference = reference;
            }


            @Override
            public ReferenceValue prepare(ColumnMetadata receiver, VariableSpecifications bindVariables)
            {
                reference.checkResolved();
                checkTrue(reference.column() != null, "substitution references must reference a column (%s)", reference);
                return new Substitution((RowDataReference) reference.prepare(null, receiver));
            }

            @Override
            public TestResult testAssignment(String keyspace, ColumnSpecification receiver)
            {
                return reference.testAssignment(keyspace, receiver);
            }

            @Override
            public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
            {
                return reference.prepare(keyspace, receiver);
            }

            @Override
            public String getText()
            {
                return reference.getText();
            }

            @Override
            public AbstractType<?> getExactTypeIfKnown(String keyspace)
            {
                return reference.getExactTypeIfKnown(keyspace);
            }
        }
    }
}
