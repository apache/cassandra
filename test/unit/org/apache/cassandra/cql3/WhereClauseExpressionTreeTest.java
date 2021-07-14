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

import org.junit.Test;

import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WhereClauseExpressionTreeTest
{
    @Test(expected = SyntaxException.class)
    public void cannotHaveEmptyWhereClause() throws Throwable
    {
        cqlParse("");
    }

    @Test
    public void singleRelationWithoutEnclosure() throws Throwable
    {
        testExpression("a = 1");
    }

    @Test
    public void singleRelationWithEnclosure() throws Throwable
    {
        testExpression("(a = 1)");
    }

    @Test
    public void simpleAndExpressionWithRelationsWithoutEnclosure() throws Throwable
    {
        testExpression("a = 1 AND b = 1");
    }

    @Test
    public void simpleAndExpressionWithRelationsWithEnclosure() throws Throwable
    {
        testExpression("(a = 1 AND b = 1)");
    }

    @Test
    public void multipleAndExpressionWithRelations() throws Throwable
    {
        testExpression("a = 1 AND b = 1 AND c = 1");
    }

    @Test
    public void disjunctionExpression() throws Throwable
    {
        testExpression("a = 1 AND b = 1 OR c = 1");
    }

    @Test
    public void test() throws Throwable
    {
        System.out.println(cqlParse("a = 1 OR b = 1 AND c = 1"));
    }

    @Test
    public void precedenceIsMaintainedWithoutParentheses() throws Throwable
    {
        testExpression("a = 1 AND b = 1 OR c = 1");

        testExpression("a = 1 OR b = 1 AND c = 1");

        testExpression("a = 1 OR b = 1 OR c = 1 AND d = 1 OR e = 1");

        testExpression("a = 1 AND b = 1 AND c = 1 OR d = 1 AND e = 1");
    }

    @Test
    public void multipleDisjunctionExpression() throws Throwable
    {
        testExpression("(a = 1 AND b = 1) OR (c = 1 AND d = 1)");
    }

    @Test
    public void disjunctionExpressionWithPrecedence() throws Throwable
    {
        testExpression("a = 1 AND (b = 1 OR (c = 1 AND d = 1 AND e = 1))");
    }

    @Test
    public void flattenConjunction() throws Throwable
    {
        WhereClause clause = WhereClause.parse("a = 1 AND (b = 1 AND c = 1)");
        WhereClause.ExpressionElement flattened = clause.root().flatten();
        assertTrue(flattened instanceof WhereClause.AndElement);
        assertEquals(3, ((WhereClause.ContainerElement) flattened).children.size());
        assertEquals("a = 1 AND b = 1 AND c = 1", flattened.toString());
    }

    @Test
    public void flattenDisjunction() throws Throwable
    {
        WhereClause clause = WhereClause.parse("a = 1 OR (b = 1 OR c = 1)");
        WhereClause.ExpressionElement flattened = clause.root().flatten();
        assertTrue(flattened instanceof WhereClause.OrElement);
        assertEquals(3, ((WhereClause.ContainerElement) flattened).children.size());
        assertEquals("a = 1 OR b = 1 OR c = 1", flattened.toString());
    }

    @Test
    public void flattenDeeplyNested() throws Throwable
    {
        WhereClause.ExpressionElement flattened;

        // deeper nesting, right
        flattened = WhereClause.parse("a = 1 OR (b = 1 OR (c = 1 OR d = 1))").root().flatten();
        assertEquals("a = 1 OR b = 1 OR c = 1 OR d = 1", flattened.toString());

        // deeper nesting, left
        flattened = WhereClause.parse("((a = 1 OR b = 1) OR c = 1) OR d = 1").root().flatten();
        assertEquals("a = 1 OR b = 1 OR c = 1 OR d = 1", flattened.toString());
    }


    @Test
    public void flattenMixed() throws Throwable
    {
        WhereClause.ExpressionElement flattened;

        flattened = WhereClause.parse("a = 1 OR (b = 1 AND c = 1)").root().flatten();
        assertEquals("a = 1 OR (b = 1 AND c = 1)", flattened.toString());

        flattened = WhereClause.parse("(a = 1 OR (b = 1 OR c = 1)) AND (d = 1 AND (e = 1 OR f = 1))").root().flatten();
        assertEquals("(a = 1 OR b = 1 OR c = 1) AND d = 1 AND (e = 1 OR f = 1)", flattened.toString());
    }

    @Test
    public void conjunctiveFormSimple() throws Throwable
    {
        WhereClause.AndElement conj = WhereClause.parse("a = 1").root().conjunctiveForm();
        assertEquals(1, conj.children.size());
        assertEquals("a = 1", conj.toString());
    }

    @Test
    public void conjunctiveFormSingleAnd() throws Throwable
    {
        WhereClause.AndElement conj = WhereClause.parse("a = 1 AND b = 1").root().conjunctiveForm();
        assertEquals(2, conj.children.size());
        assertEquals("a = 1 AND b = 1", conj.toString());
    }

    @Test
    public void conjunctiveFormSingleOr() throws Throwable
    {
        WhereClause.AndElement conj = WhereClause.parse("a = 1 OR b = 1").root().conjunctiveForm();
        assertEquals(1, conj.children.size());
        assertEquals("a = 1 OR b = 1", conj.toString());
    }

    @Test
    public void conjunctiveFormNested() throws Throwable
    {
        WhereClause.AndElement conj = WhereClause.parse("a = 1 AND (b = 1 AND c = 1)").root().conjunctiveForm();
        assertEquals(3, conj.children.size());
        assertEquals("a = 1 AND b = 1 AND c = 1", conj.toString());
    }

    @Test
    public void rename() throws Throwable
    {
        WhereClause.ExpressionElement root = WhereClause.parse("a1 = 1 OR (b1 = 1 AND c1 = 1)").root();

        WhereClause.ExpressionElement renamed1 =
                root.rename(
                        ColumnIdentifier.getInterned("a1", false),
                        ColumnIdentifier.getInterned("a2", false));

        assertEquals("a2 = 1 OR (b1 = 1 AND c1 = 1)", renamed1.toString());

        WhereClause.ExpressionElement renamed2 =
                root.rename(
                        ColumnIdentifier.getInterned("b1", false),
                        ColumnIdentifier.getInterned("b2", false));

        assertEquals("a1 = 1 OR (b2 = 1 AND c1 = 1)", renamed2.toString());
    }

    @Test
    public void randomTest() throws Throwable
    {
        for (int count = 0; count < CQLTester.getRandom().nextIntBetween(100, 1000); count++)
            testExpression(randomExpression());
    }

    private void testExpression(String expression) throws Throwable
    {
        assertEquals("Failed to correctly parse: [" + expression + "]", jboolParse(expression), jboolParse(cqlParse(expression)));
    }

   private static String alphabet = "abcdefghijklmnopqrstuvwxyz";

   private String randomExpression()
   {
       StringBuilder builder = new StringBuilder();

       boolean applyPrecedence = CQLTester.getRandom().nextBoolean();

       int numberOfElements = CQLTester.getRandom().nextIntBetween(1, 26);
       int precedenceLevel = 0;
       for (int element = 0; element < numberOfElements - 1; element++)
       {
           if (applyPrecedence && CQLTester.getRandom().nextIntBetween(0, 2) == 0)
           {
               builder.append("(");
               precedenceLevel++;
           }
           builder.append(alphabet, element, element + 1);
           builder.append(" = 1");
           if (applyPrecedence && CQLTester.getRandom().nextIntBetween(0, 2) == 2 && precedenceLevel > 0)
           {
               builder.append(")");
               precedenceLevel--;
           }
           builder.append(CQLTester.getRandom().nextBoolean() ? " AND " : " OR ");
       }
       builder.append(alphabet, numberOfElements - 1, numberOfElements);
       builder.append(" = 1");
       if (applyPrecedence)
           while (precedenceLevel-- > 0)
               builder.append(")");

       return builder.toString();
   }

   private String cqlParse(String expression) throws Throwable
   {
       return WhereClause.parse(expression).root().toString();
   }

   private String jboolParse(String expression)
   {
       return ExprParser.parse(toJbool(expression)).toString();
   }

   private String toJbool(String cqlExpression)
   {
       return cqlExpression.replaceAll("AND", "&").replaceAll("OR", "|").replaceAll(" = 1", "");
   }
}
