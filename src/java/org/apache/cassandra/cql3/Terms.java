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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;

import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;

public class Terms
{

    private static com.google.common.base.Function<Term, Iterable<Function>> TO_FUNCTION_ITERABLE =
    new com.google.common.base.Function<Term, Iterable<Function>>()
    {
        public Iterable<Function> apply(Term term)
        {
            return term.getFunctions();
        }
    };

    public static Iterable<Function> getFunctions(Iterable<Term> terms)
    {
        if (terms == null)
            return Collections.emptySet();

        return Iterables.concat(Iterables.transform(terms, TO_FUNCTION_ITERABLE));
    }

    public static ByteBuffer asBytes(String keyspace, String term, AbstractType type)
    {
        ColumnSpecification receiver = new ColumnSpecification(keyspace, "--dummy--", new ColumnIdentifier("(dummy)", true), type);
        Term.Raw rawTerm = CQLFragmentParser.parseAny(CqlParser::term, term, "CQL term");
        return rawTerm.prepare(keyspace, receiver).bindAndGet(QueryOptions.DEFAULT);
    }
}
