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

public class StandardVisitors
{
    public static final Visitor BIND_TO_LITERAL = new Visitor()
    {
        @Override
        public Value visit(Value v)
        {
            if (!(v instanceof Bind)) return v;
            Bind b = (Bind) v;
            return new Literal(b.value(), b.type());
        }
    };
    public static final Visitor LITERAL_TO_BIND = new Visitor()
    {
        @Override
        public Value visit(Value v)
        {
            if (!(v instanceof Literal)) return v;
            Literal b = (Literal) v;
            return new Bind(b.value(), b.type());
        }
    };

    public static final Visitor UNWRAP_TYPE_HINT = new Visitor()
    {
        @Override
        public Expression visit(Expression e)
        {
            if (!(e instanceof TypeHint)) return e;
            TypeHint hint = (TypeHint) e;
            if (hint.type.equals(hint.e.type()))
                return hint.e;
            return e;
        }
    };

    public static final Visitor APPLY_OPERATOR = new Visitor()
    {
        @Override
        public Expression visit(Expression e)
        {
            if (!(e instanceof Operator)) return e;
            return new Bind(ExpressionEvaluator.eval((Operator) e), e.type());
        }
    };

    public static final Visitor.CompositeVisitor DEBUG = Visitor.CompositeVisitor.of(UNWRAP_TYPE_HINT, BIND_TO_LITERAL);

    private StandardVisitors() {}
}
