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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * This is a parsed representation of the expression following the WHERE element
 * in a CQL statement. It is parsed into an arbitrary sized expression tree consisting
 * of <code>ExpressionElement</code> elements.
 */
public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(ExpressionElement.EMPTY);

    private final ExpressionElement rootElement;

    private WhereClause(ExpressionElement rootElement)
    {
        this.rootElement = rootElement;
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean containsCustomExpressions()
    {
        return rootElement.containsCustomExpressions();
    }

    public ExpressionElement root()
    {
        return rootElement;
    }

    /**
     * Renames identifiers in all relations
     *
     * @param from the old identifier
     * @param to   the new identifier
     * @return a new WhereClause with with "from" replaced by "to" in all relations
     */
    public WhereClause renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        return new WhereClause(rootElement.rename(from, to));
    }

    public static WhereClause parse(String cql) throws RecognitionException
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, cql).build();
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    /**
     * Returns a CQL representation of this WHERE clause.
     *
     * @return a CQL representation of this WHERE clause
     */
    public String toCQLString()
    {
        return rootElement.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return rootElement.toString().equals(wc.rootElement.toString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rootElement);
    }

    /**
     * Checks if the where clause contains some token relations.
     *
     * @return {@code true} if it is the case, {@code false} otherwise.
     */
    public boolean containsTokenRelations()
    {
        for (Relation rel : rootElement.relations())
        {
            if (rel.onToken())
                return true;
        }
        return false;
    }

    /**
     * This receives fragments from the parse operation and builds them into the final <code>WhereClause</code>.
     *
     * The received fragments are:
     * <ul>
     *     <li><code>add(Relation)</code> - adds a new relation to the current <code>ParseState</code></li>
     *     <li><code>add(CustomIndexExpression)</code> - adds a new custom index expression to the current <code>ParseState</code></li>
     *     <li><code>startEnclosure</code> - responds to a '(' and pushes the current <code>ParseState</code> onto the precedence stack</li>
     *     <li><code>endEnclosure</code> - responds to a ')' and pulls the <code>ParseState</code> associated with the
     *     matching <code>startEnclosure</code>. It will pull any intermediate precedence states off the stack until it
     *     reaches the matching enclosure state</li>
     *     <li><code>setCurrentOperator</code> - changes the operator in the <code>ParseState</code>. If this new operator is
     *     of a higher precedence than the current operator, the last expression is popped from the <code>ParseState</code> and
     *     the state is pushed onto the precedence stack</li>
     *     <li><code>build</code> - always the last call. This builds the resultant <code>ExpressionTree</code> from the
     *     precedence stack and the current <code>ParseState</code></li>
     * </ul>
     */
    public static final class Builder
    {
        private final Deque<ParseState> precedenceStack = new ArrayDeque<>();
        private ParseState parseState = new ParseState();

        public void add(Relation relation)
        {
            parseState.push(new RelationElement(relation));
        }

        public void add(CustomIndexExpression customIndexExpression)
        {
            parseState.push(new CustomIndexExpressionElement(customIndexExpression));
        }

        public void startEnclosure()
        {
            pushStack(PushState.ENCLOSURE);
        }

        public void endEnclosure()
        {
            do
            {
                ExpressionElement expression = generate();
                parseState = precedenceStack.pop();
                parseState.push(expression);
            }
            while (parseState.enclosure == PushState.PRECEDENCE);
        }

        public void setCurrentOperator(String value)
        {
            Operator operator = Operator.valueOf(value.toUpperCase());
            if (parseState.isChangeOfOperator(operator))
            {
                if (parseState.higherPrecedence(operator))
                {
                    // Where we have a = 1 OR b = 1 AND c = 1. When the operator changes to AND
                    // we need to pop b = 1 from the parseState, push the parseState containing
                    // a = 1 OR and then add b = 1 to the new parseState
                    ExpressionElement last = parseState.pop();
                    pushStack(PushState.PRECEDENCE);
                    parseState.push(last);
                }
                else
                {
                    ExpressionElement element = generate();
                    if (!precedenceStack.isEmpty() && precedenceStack.peek().enclosure == PushState.PRECEDENCE)
                        parseState = precedenceStack.pop();
                    else
                        parseState.clear();
                    parseState.push(element);
                }
            }
            parseState.operator = operator;
        }

        public WhereClause build()
        {
            while (!precedenceStack.isEmpty())
            {
                ExpressionElement expression = generate();
                parseState = precedenceStack.pop();
                parseState.push(expression);
            }
            return new WhereClause(generate());
        }

        private void pushStack(PushState enclosure)
        {
            parseState.enclosure = enclosure;
            precedenceStack.push(parseState);
            parseState = new ParseState();
        }

        private ExpressionElement generate()
        {
            if (parseState.size() == 1)
                return parseState.pop();
            return parseState.asContainer();
        }
    }

    /**
     * Represents the state of the parsing operation at a point of enclosure or precedence change.
     */
    public static class ParseState
    {
        Operator operator = Operator.NONE;
        PushState enclosure = PushState.NONE;
        Deque<ExpressionElement> expressionElements = new ArrayDeque<>();

        void push(ExpressionElement element)
        {
            expressionElements.add(element);
        }

        ExpressionElement pop()
        {
            return expressionElements.removeLast();
        }

        int size()
        {
            return expressionElements.size();
        }

        ParseState clear()
        {
            expressionElements.clear();
            return this;
        }

        boolean isChangeOfOperator(Operator operator)
        {
            return this.operator != operator && expressionElements.size() > 1;
        }

        boolean higherPrecedence(Operator operator)
        {
            return operator.compareTo(this.operator) > 0;
        }

        ContainerElement asContainer()
        {
            return operator == Operator.OR ? new OrElement().add(expressionElements) : new AndElement().add(expressionElements);
        }
    }

    enum Operator
    {
        NONE, OR, AND;

        public String joinValue()
        {
            return " " + name() + " ";
        }
    }

    /**
     * This is the reason why the <code>ParseState</code> was pushed onto the precedence stack.
     */
    enum PushState
    {
        NONE, PRECEDENCE, ENCLOSURE
    }

    public static abstract class ExpressionElement
    {
        private static final ExpressionElement EMPTY = new EmptyElement();

        public List<ContainerElement> operations()
        {
            return Collections.emptyList();
        }

        public boolean isDisjunction()
        {
            return false;
        }

        public List<Relation> relations()
        {
            return Collections.emptyList();
        }

        public List<CustomIndexExpression> expressions()
        {
            return Collections.emptyList();
        }

        public boolean containsCustomExpressions()
        {
            return false;
        }

        public abstract String toEncapsulatedString();

        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            return this;
        }
    }

    public static abstract class VariableElement extends ExpressionElement
    {
        @Override
        public String toEncapsulatedString()
        {
            return toString();
        }
    }

    public static class EmptyElement extends VariableElement
    {
        @Override
        public String toString()
        {
            return "";
        }
    }

    public static class RelationElement extends VariableElement
    {
        private final Relation relation;

        public RelationElement(Relation relation)
        {
            this.relation = relation;
        }

        @Override
        public List<Relation> relations()
        {
            return Lists.newArrayList(relation);
        }

        @Override
        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            return new RelationElement(relation.renameIdentifier(from, to));
        }

        @Override
        public String toString()
        {
            return relation.toString();
        }
    }

    public static class CustomIndexExpressionElement extends VariableElement
    {
        private final CustomIndexExpression customIndexExpression;

        public CustomIndexExpressionElement(CustomIndexExpression customIndexExpression)
        {
            this.customIndexExpression = customIndexExpression;
        }

        @Override
        public List<CustomIndexExpression> expressions()
        {
            return Lists.newArrayList(customIndexExpression);
        }

        @Override
        public boolean containsCustomExpressions()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return customIndexExpression.toString();
        }
    }

    public static abstract class ContainerElement extends ExpressionElement
    {
        protected final List<ExpressionElement> children = new ArrayList<>();

        @Override
        public List<ContainerElement> operations()
        {
            return children.stream()
                           .filter(c -> (c instanceof ContainerElement))
                           .map(r -> ((ContainerElement) r))
                           .collect(Collectors.toList());
        }

        public ContainerElement add(Deque<ExpressionElement> children)
        {
            this.children.addAll(children);
            return this;
        }

        protected abstract Operator operator();

        @Override
        public List<Relation> relations()
        {
            return children.stream()
                           .filter(c -> (c instanceof RelationElement))
                           .map(r -> (((RelationElement) r).relation))
                           .collect(Collectors.toList());
        }

        @Override
        public List<CustomIndexExpression> expressions()
        {
            return children.stream()
                           .filter(c -> (c instanceof CustomIndexExpressionElement))
                           .map(r -> (((CustomIndexExpressionElement) r).customIndexExpression))
                           .collect(Collectors.toList());
        }

        @Override
        public boolean containsCustomExpressions()
        {
            return children.stream().anyMatch(ExpressionElement::containsCustomExpressions);
        }

        @Override
        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            AndElement element = new AndElement();
            children.stream().map(c -> c.rename(from, to)).forEach(c -> element.children.add(c));
            return element;
        }

        @Override
        public String toString()
        {
            return children.stream().map(ExpressionElement::toEncapsulatedString).collect(Collectors.joining(operator().joinValue()));
        }

        @Override
        public String toEncapsulatedString()
        {
            return children.stream().map(ExpressionElement::toEncapsulatedString).collect(Collectors.joining(operator().joinValue(), "(", ")"));
        }
    }

    public static class AndElement extends ContainerElement
    {
        @Override
        protected Operator operator()
        {
            return Operator.AND;
        }
    }

    public static class OrElement extends ContainerElement
    {
        @Override
        protected Operator operator()
        {
            return Operator.OR;
        }

        @Override
        public boolean isDisjunction()
        {
            return true;
        }
    }
}
