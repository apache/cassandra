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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import accord.utils.Invariants;

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
        if (e instanceof ReferenceExpression) return visit((ReferenceExpression) e);
        return e;
    }

    default Conditional visit(Conditional c) { return c; }

    default ReferenceExpression visit(ReferenceExpression r)
    {
        return r;
    }

    default Value visit(Value v) { return v; }

    default CasCondition visit(CasCondition s)
    {
        return s;
    }

    class CompositeVisitor implements Visitor
    {
        private final List<Visitor> visitors;

        private CompositeVisitor(List<Visitor> visitors)
        {
            this.visitors = visitors;
        }

        public static CompositeVisitor of(Visitor... visitors)
        {
            return of(Arrays.asList(visitors));
        }

        public static CompositeVisitor of(List<Visitor> visitors)
        {
            Invariants.requireArgument(!visitors.isEmpty(), "Visitors may not be empty");

            if (Stream.of(visitors).noneMatch(v -> v instanceof CompositeVisitor))
                return new CompositeVisitor(visitors);
            List<Visitor> flatten = new ArrayList<>();
            for (Visitor v : visitors)
            {
                if (!(v instanceof CompositeVisitor))
                {
                    flatten.add(v);
                    continue;
                }
                CompositeVisitor cv = (CompositeVisitor) v;
                flatten.addAll(cv.visitors);
            }
            return new CompositeVisitor(flatten);
        }

        public CompositeVisitor append(Visitor v)
        {
            if (v instanceof CompositeVisitor)
            {
                CompositeVisitor other = (CompositeVisitor) v;
                List<Visitor> vs = new ArrayList<>(visitors.size() + other.visitors.size());
                vs.addAll(visitors);
                vs.addAll(other.visitors);
                return new CompositeVisitor(vs);
            }
            else
            {
                List<Visitor> vs = new ArrayList<>(visitors.size() + 1);
                vs.addAll(visitors);
                vs.add(v);
                return new CompositeVisitor(vs);
            }
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
