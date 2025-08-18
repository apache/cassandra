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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.cql3.ast.Conditional.And;
import org.apache.cassandra.cql3.ast.Conditional.Where;
import org.apache.cassandra.db.marshal.Int32Type;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class ExpressionTest
{
    private static final Visitor.CompositeVisitor COMPOSITE_VISITOR = Visitor.CompositeVisitor.of(StandardVisitors.BIND_TO_LITERAL);

    @Test
    public void simple()
    {
        qt().forAll(Gens.random(), expressions()).check((rs, ex) -> {
            Visitor v = rs.nextBoolean() ? StandardVisitors.BIND_TO_LITERAL : COMPOSITE_VISITOR;
            Expression update = ex.visit(v);
            assertNoBind(update);
        });
    }

    @Test
    public void select()
    {
        assertNoBind(selectWithBind().visit(StandardVisitors.BIND_TO_LITERAL));
    }

    @Test
    public void txn()
    {
        Txn txn = Txn.builder()
                     .addLet("a", selectWithBind())
                     .addReturn(selectWithBind())
                     .addIf(new Conditional.Is(Symbol.unknownType("a"), Conditional.Is.Kind.NotNull),
                            new Mutation.Insert(new TableReference(Optional.empty(), "tbl"), new LinkedHashMap<>(Map.of(Symbol.unknownType("a"), Bind.of(0))), false, Optional.empty()))
                     .build();
        assertNoBind(txn.visit(StandardVisitors.BIND_TO_LITERAL));

        txn = Txn.builder()
                 .addLet("a", selectWithBind())
                 .addReturn(selectWithBind())
                     .addIf(Where.create(Where.Inequality.EQUAL, Bind.of(0), Bind.of(42)),
                        new Mutation.Insert(new TableReference(Optional.empty(), "tbl"), new LinkedHashMap<>(Map.of(Symbol.unknownType("a"), Bind.of(0))), false, Optional.empty()))
                 .build();
        assertNoBind(txn.visit(StandardVisitors.BIND_TO_LITERAL));
    }

    private static Select selectWithBind()
    {
        return Select.builder()
                     .table("ks", "tbl")
                     .value("foo", 42)
                     .limit(1)
                     .build();
    }

    private static void assertNoBind(Element update)
    {
        Assertions.assertThat(update
                              .streamRecursive(true)
                              .filter(e -> e instanceof Bind)
                              .count()).isEqualTo(0);
    }

    private static Gen<Conditional> conditionals()
    {
        return rs -> {
            // in / where / and
            switch (rs.nextInt(0, 2))
            {
                case 0:
                    return new Conditional.In(new Symbol("col", Int32Type.instance), Gens.lists(expressions()).ofSizeBetween(1, 3).next(rs));
                case 1:
                    return Where.create(Where.Inequality.EQUAL, new Symbol("col", Int32Type.instance), expressions().next(rs));
                case 2:
                {
                    var gen = conditionals();
                    return new And(gen.next(rs), gen.next(rs));
                }
                default:
                    throw new UnsupportedOperationException();
            }
        };
    }

    private static Gen<Expression> expressions()
    {
        return rs -> {
            var subExpression = expressions().filter(e -> e.type() == Int32Type.instance);
            if (rs.decide(.1))
            {
                // operator
                return new Operator(Operator.Kind.ADD, subExpression.next(rs), subExpression.next(rs));
            }
            if (rs.decide(.1))
            {
                return new AssignmentOperator(AssignmentOperator.Kind.ADD, subExpression.next(rs));
            }
            if (rs.decide(.1))
                return new Cast(subExpression.next(rs), Int32Type.instance);
            if (rs.decide(.1))
                return new FunctionCall("fn", Gens.lists(subExpression).ofSizeBetween(1, 10).next(rs), Int32Type.instance);
            if (rs.decide(.1))
                return new TypeHint(subExpression.next(rs));
            if (rs.decide(.1))
                return conditionals().next(rs);
            int value = rs.nextInt();
            return rs.nextBoolean() ? Literal.of(value) : Bind.of(value);
        };
    }
}
