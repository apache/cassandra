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

import java.util.List;

/**
 * Visits and conditionally replaces an element in the ast.  Each "visit" method offers the ability to be notified when the
 * desired type is found in the ast tree, and allows the visitor to replace the current element (return the element unchanged
 * if no mutations are desired, return a different element if a change is desired).
 *
 * For elements that support visitors, the following pattern should be respected.
 * <ol>
 *     <li>check the top level element first
 *         {code}
 *         var u = visitor.visit(this);
 *         if (u != this) return u;
 *     {code}</li>
 *     <li>check each sub element; replacing the top level element if any sub-elements are changed
 *     {code}
 *     boolean updated = false;
 *     var a = a.visit(visitor);
 *     updated |= (a != this.a);
 *     ...
 *     return !updated ? this : copy...
 *     {code}
 *     </li>
 * </ol>
 */
public interface Visitor
{
    default Statement visit(Statement s)
    {
        return s;
    }

    default Expression visit(Expression e)
    {
        if (e instanceof Value) return visit((Value) e);
        return e;
    }

    default Conditional visit(Conditional c) { return c; }

    default Value visit(Value v) { return v; }

    class CompositeVisitor implements Visitor
    {
        private final List<Visitor> visitors;

        public CompositeVisitor(List<Visitor> visitors)
        {
            this.visitors = visitors;
        }

        @Override
        public Expression visit(Expression e)
        {
            for (var v : visitors)
                e = v.visit(e);
            return e;
        }

        @Override
        public Conditional visit(Conditional c)
        {
            for (var v : visitors)
                c = v.visit(c);
            return c;
        }

        @Override
        public Statement visit(Statement s)
        {
            for (var v : visitors)
                s = v.visit(s);
            return s;
        }

        @Override
        public Value visit(Value c)
        {
            for (var v : visitors)
                c = v.visit(c);
            return c;
        }
    }
}
