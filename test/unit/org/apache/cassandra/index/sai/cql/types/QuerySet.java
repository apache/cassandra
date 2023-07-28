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
package org.apache.cassandra.index.sai.cql.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public abstract class QuerySet extends CQLTester
{
    final DataSet<?> dataset;

    QuerySet(DataSet<?> dataset)
    {
        this.dataset = dataset;
    }

    public abstract void runQueries(SAITester tester, Object[][] allRows) throws Throwable;

    public static class NumericQuerySet extends QuerySet
    {
        NumericQuerySet(DataSet<?> dataset)
        {
            super(dataset);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            // Query each value for all operators
            for (int index = 0; index < allRows.length; index++)
            {
                assertRows(tester.execute("SELECT * FROM %s WHERE value = ?", allRows[index][2]), new Object[][] { allRows[index] });
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value > ?", allRows[index][2]), Arrays.copyOfRange(allRows, index + 1, allRows.length));
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value >= ?", allRows[index][2]), Arrays.copyOfRange(allRows, index, allRows.length));
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value < ?", allRows[index][2]), Arrays.copyOfRange(allRows, 0, index));
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value <= ?", allRows[index][2]), Arrays.copyOfRange(allRows, 0, index + 1));
            }

            // Query full range
            assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value >= ? AND value <= ?", allRows[0][2], allRows[NUMBER_OF_VALUES - 1][2]), allRows);

            // Query random ranges. This selects a series of random ranges and tests the different possible inclusivity
            // on them. This loops a reasonable number of times to cover as many ranges as possible without taking too long
            for (int range = 0; range < allRows.length / 4; range++)
            {
                int index1 = 0;
                int index2 = 0;
                while (index1 == index2)
                {
                    index1 = getRandom().nextIntBetween(0, allRows.length - 1);
                    index2 = getRandom().nextIntBetween(0, allRows.length - 1);
                }

                int min = Math.min(index1, index2);
                int max = Math.max(index1, index2);

                // lower exclusive -> upper exclusive
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value > ? AND value < ?", allRows[min][2], allRows[max][2]),
                        Arrays.copyOfRange(allRows, min + 1, max));

                // lower inclusive -> upper exclusive
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value >= ? AND value < ?", allRows[min][2], allRows[max][2]),
                        Arrays.copyOfRange(allRows, min, max));

                // lower exclusive -> upper inclusive
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value > ? AND value <= ?", allRows[min][2], allRows[max][2]),
                        Arrays.copyOfRange(allRows, min + 1, max + 1));

                // lower inclusive -> upper inclusive
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value >= ? AND value <= ?", allRows[min][2], allRows[max][2]),
                        Arrays.copyOfRange(allRows, min, max + 1));
            }
        }
    }

    public static class LiteralQuerySet extends QuerySet
    {
        LiteralQuerySet(DataSet<?> dataSet)
        {
            super(dataSet);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            // Query each value for EQ operator
            for (int index = 0; index < allRows.length; index++)
            {
                assertRows(tester.execute("SELECT * FROM %s WHERE value = ?", allRows[index][2]), new Object[][] { allRows[index] });
            }
        }
    }

    public static class CollectionQuerySet extends QuerySet
    {
        protected DataSet<?> elementDataSet;

        public CollectionQuerySet(DataSet<?> dataSet, DataSet<?> elementDataSet)
        {
            super(dataSet);
            this.elementDataSet = elementDataSet;
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ?",
                        elementDataSet.values[index]), getExpectedRows(elementDataSet.values[index], allRows));
            }

            for (int and = 0; and < allRows.length / 4; and++)
            {
                int index = getRandom().nextIntBetween(0, allRows.length - 1);
                Iterator valueIterator = ((Collection) allRows[index][2]).iterator();
                Object value1 = valueIterator.next();
                Object value2 = valueIterator.hasNext() ? valueIterator.next() : value1;
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ? AND value CONTAINS ?",
                    value1, value2), getExpectedRows(value1, value2, allRows));
            }
        }

        protected Object[][] getExpectedRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Collection)row[2]).contains(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedRows(Object value1, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Collection)row[2]).contains(value1) && ((Collection)row[2]).contains(value2))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }
    }

    public static class FrozenCollectionQuerySet extends QuerySet
    {
        public FrozenCollectionQuerySet(DataSet<?> dataset)
        {
            super(dataset);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value = ?",
                        allRows[index][2]), getExpectedRows(allRows[index][2], allRows));
            }
        }

        protected Object[][] getExpectedRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (row[2].equals(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }
    }

    public static class MapValuesQuerySet extends CollectionQuerySet
    {
        public MapValuesQuerySet(DataSet<?> dataSet, DataSet<?> elementDataSet)
        {
            super(dataSet, elementDataSet);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ?",
                        elementDataSet.values[index]), getExpectedRows(elementDataSet.values[index], allRows));
            }

            for (int and = 0; and < allRows.length / 4; and++)
            {
                int index = getRandom().nextIntBetween(0, allRows.length - 1);
                Map map = (Map)allRows[index][2];
                Object value1 = map.values().toArray()[getRandom().nextIntBetween(0, map.values().size() - 1)];
                Object value2 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.values().size() - 1)];
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ? AND value CONTAINS ?",
                        value1, value2), getExpectedRows(value1, value2, allRows));
            }
        }

        protected Object[][] getExpectedRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).values().contains(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedRows(Object value1, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).values().contains(value1) && ((Map)row[2]).values().contains(value2))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }
    }

    public static class MapKeysQuerySet extends CollectionQuerySet
    {
        public MapKeysQuerySet(DataSet<?> dataSet, DataSet<?> elementDataSet)
        {
            super(dataSet, elementDataSet);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS KEY ?",
                        elementDataSet.values[index]), getExpectedRows(elementDataSet.values[index], allRows));
            }

            for (int and = 0; and < allRows.length / 4; and++)
            {
                int index = getRandom().nextIntBetween(0, allRows.length - 1);
                Map map = (Map)allRows[index][2];
                Object key1 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                Object key2 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS KEY ? AND value CONTAINS KEY ?",
                        key1, key2), getExpectedRows(key1, key2, allRows));
            }
        }

        protected Object[][] getExpectedRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).keySet().contains(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedRows(Object value1, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).keySet().contains(value1) && ((Map)row[2]).keySet().contains(value2))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }
    }

    public static class MapEntriesQuerySet extends CollectionQuerySet
    {
        public MapEntriesQuerySet(DataSet<?> dataSet, DataSet<?> elementDataSet)
        {
            super(dataSet, elementDataSet);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                Map map = (Map)allRows[index][2];
                Object key = map.keySet().toArray()[0];
                Object value = map.get(key);
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value[?] = ?",
                        key, value), getExpectedRows(key, value, allRows));
            }
            for (int and = 0; and < allRows.length / 4; and++)
            {
                int index = getRandom().nextIntBetween(0, allRows.length - 1);
                Map map = (Map)allRows[index][2];
                Object key1 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                Object value1 = map.get(key1);
                Object key2 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                Object value2 = map.get(key2);
                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value[?] = ? AND value[?] = ?",
                        key1, value1, key2, value2), getExpectedRows(key1, value1, key2, value2, allRows));
            }
        }

        protected Object[][] getExpectedRows(Object key, Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                Map rowMap = (Map)row[2];
                if (rowMap.containsKey(key))
                {
                    if (rowMap.get(key).equals(value))
                        expected.add(row);
                }
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedRows(Object key1, Object value1, Object key2, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                Map rowMap = (Map)row[2];
                if (rowMap.containsKey(key1) && rowMap.containsKey(key2))
                {
                    if (rowMap.get(key1).equals(value1) && rowMap.get(key2).equals(value2))
                        expected.add(row);
                }
            }
            return expected.toArray(new Object[][]{});
        }
    }

    public static class MultiMapQuerySet extends CollectionQuerySet
    {
        public MultiMapQuerySet(DataSet<?> dataSet, DataSet<?> elementDataSet)
        {
            super(dataSet, elementDataSet);
        }

        @Override
        public void runQueries(SAITester tester, Object[][] allRows) throws Throwable
        {
            for (int index = 0; index < allRows.length; index++)
            {
                Map map = (Map)allRows[index][2];
                Object key = map.keySet().toArray()[0];
                Object value = map.get(key);

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS KEY ?", key),
                        getExpectedKeyRows(key, allRows));

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ?", value),
                        getExpectedValueRows(value, allRows));

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value[?] = ?", key, value),
                        getExpectedEntryRows(key, value, allRows));
            }
            for (int and = 0; and < allRows.length / 4; and++)
            {
                int index = getRandom().nextIntBetween(0, allRows.length - 1);
                Map map = (Map)allRows[index][2];
                Object key1 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                Object value1 = map.get(key1);
                Object key2 = map.keySet().toArray()[getRandom().nextIntBetween(0, map.keySet().size() - 1)];
                Object value2 = map.get(key2);

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS KEY ? AND value CONTAINS KEY ?", key1, key2),
                                        getExpectedKeyRows(key1, key2, allRows));

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value CONTAINS ? AND value CONTAINS ?", value1, value2),
                                        getExpectedValueRows(value1, value2, allRows));

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value[?] = ? AND value[?] = ?", key1, value1, key2, value2),
                                        getExpectedEntryRows(key1, value1, key2, value2, allRows));

                assertRowsIgnoringOrder(tester.execute("SELECT * FROM %s WHERE value[?] = ? AND value CONTAINS KEY ? AND value CONTAINS ?", key1, value1, key2, value2),
                        getExpectedMixedRows(key1, value1, key2, value2, allRows));
            }
        }

        protected Object[][] getExpectedKeyRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).keySet().contains(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedValueRows(Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).values().contains(value))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedEntryRows(Object key, Object value, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                Map rowMap = (Map)row[2];
                if (rowMap.containsKey(key))
                {
                    if (rowMap.get(key).equals(value))
                        expected.add(row);
                }
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedKeyRows(Object value1, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).keySet().contains(value1) && ((Map)row[2]).keySet().contains(value2))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedValueRows(Object value1, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                if (((Map)row[2]).values().contains(value1) && ((Map)row[2]).values().contains(value2))
                    expected.add(row);
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedEntryRows(Object key1, Object value1, Object key2, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                Map rowMap = (Map)row[2];
                if (rowMap.containsKey(key1) && rowMap.containsKey(key2))
                {
                    if (rowMap.get(key1).equals(value1) && rowMap.get(key2).equals(value2))
                        expected.add(row);
                }
            }
            return expected.toArray(new Object[][]{});
        }

        protected Object[][] getExpectedMixedRows(Object key1, Object value1, Object key2, Object value2, Object[][] allRows)
        {
            List<Object[]> expected = new ArrayList<>();
            for (Object[] row : allRows)
            {
                Map rowMap = (Map)row[2];
                if (rowMap.containsKey(key1) && rowMap.containsKey(key2) && rowMap.containsValue(value2))
                {
                    if (rowMap.get(key1).equals(value1))
                        expected.add(row);
                }
            }
            return expected.toArray(new Object[][]{});
        }
    }
}

