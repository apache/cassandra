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

package org.apache.cassandra.cql3.ast;

import java.util.stream.Stream;

public interface CasCondition extends Element
{
    CasCondition visit(Visitor v);

    default String debugCQL()
    {
        return visit(StandardVisitors.DEBUG).toCQL();
    }

    enum Simple implements CasCondition
    {
        NotExists("IF NOT EXISTS"),
        Exists("IF EXISTS");

        private final String cql;

        Simple(String s)
        {
            this.cql = s;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append(cql);
        }

        @Override
        public CasCondition visit(Visitor v)
        {
            return v.visit(this);
        }
    }

    class IfCondition implements CasCondition
    {
        public final Conditional conditional;

        public IfCondition(Conditional conditional)
        {
            this.conditional = conditional;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("IF ");
            conditional.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(conditional);
        }

        @Override
        public CasCondition visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            var c = conditional.visit(v);
            if (c == conditional) return this;
            return new IfCondition(c);
        }

        @Override
        public String toString()
        {
            return toCQL();
        }
    }
}
