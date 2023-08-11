/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.cql;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.utils.Pair;

class BaseDataModel
{
    public static final String KEYSPACE = "sai_query_keyspace";

    public static final String SIMPLE_SELECT_TEMPLATE = "SELECT %s FROM %%s WHERE %s %s ? LIMIT ?";
    public static final String RANGE_QUERY_TEMPLATE = "SELECT %s FROM %%s WHERE %s > ? AND %<s < ? LIMIT ?";
    public static final String TWO_CLAUSE_AND_QUERY_TEMPLATE = "SELECT %s FROM %%s WHERE %s %s ? AND %s %s ? LIMIT ?";
    public static final String THREE_CLAUSE_AND_QUERY_TEMPLATE = "SELECT %s FROM %%s WHERE %s %s ? AND %s %s ? AND %s %s ? LIMIT ?";

    public static final String ASCII_COLUMN = "abbreviation";
    public static final String BIGINT_COLUMN = "gdp";
    public static final String BOOLEAN_COLUMN = "active";
    public static final String DATE_COLUMN = "visited";
    public static final String DOUBLE_COLUMN = "area_sq_miles";
    public static final String FLOAT_COLUMN = "murder_rate";
    public static final String INET_COLUMN = "ip";
    public static final String INT_COLUMN = "population";
    public static final String SMALLINT_COLUMN = "murders_per_year";
    public static final String TINYINT_COLUMN = "tiny_murders_per_year";
    public static final String TEXT_COLUMN = "name";
    public static final String TIME_COLUMN = "avg_dmv_wait";
    public static final String TIMESTAMP_COLUMN = "visited_timestamp";
    public static final String UUID_COLUMN = "id";
    public static final String TIMEUUID_COLUMN = "temporal_id";
    public static final String NON_INDEXED_COLUMN = "non_indexed";

    public static final int DEFAULT_TTL_SECONDS = 10;

    public static final List<Pair<String, String>> NORMAL_COLUMNS =
            ImmutableList.<Pair<String, String>>builder()
                    .add(Pair.create(ASCII_COLUMN, CQL3Type.Native.ASCII.toString()))
                    .add(Pair.create(BIGINT_COLUMN, CQL3Type.Native.BIGINT.toString()))
                    .add(Pair.create(BOOLEAN_COLUMN, CQL3Type.Native.BOOLEAN.toString()))
                    .add(Pair.create(DATE_COLUMN, CQL3Type.Native.DATE.toString()))
                    .add(Pair.create(DOUBLE_COLUMN, CQL3Type.Native.DOUBLE.toString()))
                    .add(Pair.create(FLOAT_COLUMN, CQL3Type.Native.FLOAT.toString()))
                    .add(Pair.create(INET_COLUMN, CQL3Type.Native.INET.toString()))
                    .add(Pair.create(INT_COLUMN, CQL3Type.Native.INT.toString()))
                    .add(Pair.create(SMALLINT_COLUMN, CQL3Type.Native.SMALLINT.toString()))
                    .add(Pair.create(TINYINT_COLUMN, CQL3Type.Native.TINYINT.toString()))
                    .add(Pair.create(TEXT_COLUMN, CQL3Type.Native.TEXT.toString()))
                    .add(Pair.create(TIME_COLUMN, CQL3Type.Native.TIME.toString()))
                    .add(Pair.create(TIMESTAMP_COLUMN, CQL3Type.Native.TIMESTAMP.toString()))
                    .add(Pair.create(UUID_COLUMN, CQL3Type.Native.UUID.toString()))
                    .add(Pair.create(TIMEUUID_COLUMN, CQL3Type.Native.TIMEUUID.toString()))
                    .add(Pair.create(NON_INDEXED_COLUMN, CQL3Type.Native.INT.toString()))
                    .build();

    public static final List<String> NORMAL_COLUMN_DATA =
            ImmutableList.<String>builder()
                    .add("'AK',   500000000,  true, '2009-07-15', 570640.95,  7.7,   '158.145.20.64',   737709,  164,  16,        'Alaska', '00:18:20', '2009-07-15T00:00:00', e37394dc-d17b-11e8-a8d5-f2801f1b9fd1, acfe5ada-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'AL',  1000000000,  true, '2011-09-13',  50645.33,  7.0,   '206.16.212.91',  4853875,   57,   5,       'Alabama', '01:04:00', '2011-09-13T00:00:00', b7373af6-d7c1-45ae-b145-5bf4b5cdd00c, c592c37e-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'AR',  2000000000, false, '2013-06-17', 113594.08,  5.5,  '170.94.194.134',  2977853,   99,   9,      'Arkansas', '00:55:23', '2013-06-17T00:00:00', a0daaeb4-c8a2-4c68-9899-e32d08238550, cfaae67a-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'CA',  3000000000,  true, '2012-06-17', 155779.22,  4.8,    '67.157.98.46', 38993940, 1861, 117,    'California', '01:30:45', '2012-06-17T00:00:00', 96232af0-0af7-438b-9049-c5a5a944ff93, d7e80692-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'DE',  4000000000, false, '2013-06-17',   1948.54,  6.7,   '167.21.128.20',   944076,   63,   6,      'Delaware', '00:23:45', '2013-06-17T00:00:00', b2a0a879-5223-40d2-9671-775ee209b6f2, dd10a5b6-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'ID',  4500000000, false, '2015-06-18',  82643.12,  1.8,   '164.165.67.10',  1652828,   30,   3,         'Idaho', '00:18:45', '2015-06-18T00:00:00', c6eec0b0-0eef-40e8-ac38-3a82110443e4, e2788780-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'KY',  4750000000, false, '2018-03-12',  39486.34,  4.7,  '205.204.196.64',  4424611,  209,  20,      'Kentucky', '00:45:00', '2018-03-12T00:00:00', 752355f8-405b-4d94-88f3-9992cda30f1e, e7c4e1d4-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'LA',  4800000000,  true, '2013-06-10',  43203.90, 10.2,  '204.196.242.71',  4668960,  474,  47,     'Louisiana', '00:56:07', '2013-06-10T00:00:00', 17be691a-c1a4-4467-a4ad-64605c74fb1c, ee6136d2-d17c-11e8-a8d5-f2801f1b9fd1, 1")
                    .add("'MA',  5000000000,  true,  '2010-07-04',   7800.06,  1.9,   '170.63.206.57',  6784240,  126,  12, 'Massachusetts', '01:01:34', '2010-07-04T00:00:00', e8a3c287-78cf-46b5-b554-42562e7dcfb3, f57a3b62-d17c-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'MI',  6000000000, false, '2011-09-13',  56538.90,  5.8,    '23.72.184.64',  9917715,  571,  57,      'Michigan', '00:43:09', '2011-09-13T00:00:00', a0daaeb4-c8a2-4c68-9899-e32d08238550, 0497b886-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'MS',  7000000000,  true, '2013-06-17',  46923.27,  5.3,   '192.251.58.38',  2989390,  159,  15,   'Mississippi', '01:04:23', '2013-06-17T00:00:00', 96232af0-0af7-438b-9049-c5a5a944ff93, 0b0205e6-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'TN',  8000000000, false, '2018-03-10',  41234.90,  6.1, '170.141.221.177',  6595056,  402,  40,     'Tennessee', '00:39:45', '2018-03-10T00:00:00', b2a0a879-5223-40d2-9671-775ee209b6f2, 105dc746-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'TX',  9000000000,  true, '2014-06-17', 261231.71,  4.7,   '204.66.40.181', 27429639, 1276, 107,         'Texas', '00:38:13', '2014-06-17T00:00:00', c6eec0b0-0eef-40e8-ac38-3a82110443e4, 155b6bcc-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'UT',  9250000000,  true, '2014-06-20',  82169.62,  1.8,   '204.113.13.48',  2990632,   54,   5,          'Utah', '00:25:00', '2014-06-20T00:00:00', 752355f8-405b-4d94-88f3-9992cda30f1e, 1a267c50-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'VA',  9500000000,  true, '2018-06-19',  39490.09,  4.6,  '152.130.96.221',  8367587,  383,  38,      'Virginia', '00:43:07', '2018-06-19T00:00:00', 17be691a-c1a4-4467-a4ad-64605c74fb1c, 1fc81a4c-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .add("'WY', 10000000000, false, '2015-06-17',  97093.14,  2.7,  '192.146.215.91',   586107,   57,   5,       'Wyoming', '00:15:50', '2015-06-17T00:00:00', e8a3c287-78cf-46b5-b554-42562e7dcfb3, 2576612e-d17d-11e8-a8d5-f2801f1b9fd1, 2")
                    .build();

    public static final String STATIC_INT_COLUMN = "entered";

    public static final List<Pair<String, String>> STATIC_COLUMNS =
            ImmutableList.<Pair<String, String>>builder().add(Pair.create(STATIC_INT_COLUMN, CQL3Type.Native.INT + " static"))
                                                         .addAll(NORMAL_COLUMNS).build();

    public static final List<String> STATIC_COLUMN_DATA = ImmutableList.of("1819, " + NORMAL_COLUMN_DATA.get(0),
                                                       "1819, " + NORMAL_COLUMN_DATA.get(1),
                                                       "1850, " + NORMAL_COLUMN_DATA.get(2),
                                                       "1850, " + NORMAL_COLUMN_DATA.get(3),
                                                       "1910, " + NORMAL_COLUMN_DATA.get(4),
                                                       "1910, " + NORMAL_COLUMN_DATA.get(5),
                                                       "1792, " + NORMAL_COLUMN_DATA.get(6),
                                                       "1792, " + NORMAL_COLUMN_DATA.get(7),
                                                       "1788, " + NORMAL_COLUMN_DATA.get(8),
                                                       "1788, " + NORMAL_COLUMN_DATA.get(9),
                                                       "1817, " + NORMAL_COLUMN_DATA.get(10),
                                                       "1817, " + NORMAL_COLUMN_DATA.get(11),
                                                       "1896, " + NORMAL_COLUMN_DATA.get(12),
                                                       "1896, " + NORMAL_COLUMN_DATA.get(13),
                                                       "1845, " + NORMAL_COLUMN_DATA.get(14),
                                                       "1845, " + NORMAL_COLUMN_DATA.get(15));

    private static final AtomicInteger seq = new AtomicInteger();

    private final String columnNames;
    private final List<String> rows;

    protected final Set<String> skipColumns = Sets.newHashSet(NON_INDEXED_COLUMN, BOOLEAN_COLUMN);
    protected final List<Pair<String, String>> columns;
    protected final String indexedTable = "table_" + seq.getAndIncrement();
    protected final String nonIndexedTable = "table_" + seq.getAndIncrement();
    protected String tableOptions = "";
    protected List<Pair<String, String>> keyColumns;
    protected String primaryKey;
    protected List<String> keys;

    public BaseDataModel(List<Pair<String, String>> columns, List<String> rows)
    {
        this.keyColumns = ImmutableList.of(Pair.create("p", "int"));
        this.primaryKey = keyColumns.stream().map(pair -> pair.left).collect(Collectors.joining(", "));

        this.columns = columns;
        this.columnNames = columns.stream().map(pair -> pair.left).collect(Collectors.joining(", "));
        this.rows = rows;

        this.keys = new SimplePrimaryKeyList(rows.size());
    }

    public List<Pair<String, String>> keyColumns()
    {
        return keyColumns;
    }

    public void createTables(Executor tester)
    {
        String keyColumnDefs = keyColumns.stream().map(column -> column.left + ' ' + column.right).collect(Collectors.joining(", "));
        String normalColumnDefs = columns.stream().map(column -> column.left + ' ' + column.right).collect(Collectors.joining(", "));

        String template = "CREATE TABLE %s.%s (%s, %s, PRIMARY KEY (%s))" + tableOptions;
        tester.createTable(String.format(template, KEYSPACE, indexedTable, keyColumnDefs, normalColumnDefs, primaryKey));
        tester.createTable(String.format(template, KEYSPACE, nonIndexedTable, keyColumnDefs, normalColumnDefs, primaryKey));
    }

    public void truncateTables(Executor tester) throws Throwable
    {
        executeLocal(tester, "TRUNCATE TABLE %s");
    }

    public void createIndexes(Executor tester) throws Throwable
    {
        createIndexes(tester, columns);
    }

    protected void createIndexes(Executor tester, List<Pair<String, String>> columns) throws Throwable
    {
        String indexNameTemplate = "sai_%s_index_%s";
        String createIndexTemplate = "CREATE CUSTOM INDEX %s ON %%s (%s) USING 'StorageAttachedIndex'";

        for (Pair<String, String> column : columns)
        {
            String columnName = column.left;
            if (!skipColumns.contains(columnName))
            {
                String indexName = String.format(indexNameTemplate, columnName, indexedTable);
                executeLocalIndexed(tester, String.format(createIndexTemplate, indexName, columnName));
                tester.waitForIndexQueryable(KEYSPACE, indexName);
            }
        }
    }

    public void flush(Executor tester) throws Throwable
    {
        tester.flush(KEYSPACE, indexedTable);
        tester.flush(KEYSPACE, nonIndexedTable);
    }

    public void disableCompaction(Executor tester) throws Throwable
    {
        tester.disableCompaction(KEYSPACE, indexedTable);
        tester.disableCompaction(KEYSPACE, nonIndexedTable);
    }

    public void compact(Executor tester) throws Throwable
    {
        tester.compact(KEYSPACE, indexedTable);
        tester.compact(KEYSPACE, nonIndexedTable);
    }

    public void insertRows(Executor tester) throws Throwable
    {
        String template = "INSERT INTO %%s (%s, %s) VALUES (%s, %s)";

        for (int i = 0; i < keys.size(); i++)
        {
            executeLocal(tester, String.format(template, primaryKey, columnNames, keys.get(i), rows.get(i)));
        }
    }

    public void insertRowsWithTTL(Executor tester) throws Throwable
    {
        String template = "INSERT INTO %%s (%s, %s) VALUES (%s, %s)%s";

        for (int i = 0; i < keys.size(); i++)
        {
            String ttl = deletable().contains(i) ? " USING TTL " + DEFAULT_TTL_SECONDS : "";
            executeLocal(tester, String.format(template, primaryKey, columnNames, keys.get(i), rows.get(i), ttl));
        }
    }

    public void updateCells(Executor tester) throws Throwable
    {
        executeLocal(tester, String.format("UPDATE %%s SET %s = 9700000000 WHERE p = 0", BIGINT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = false WHERE p = 1", BOOLEAN_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = '2018-03-10' WHERE p = 2", DATE_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 8788.06 WHERE p = 3", DOUBLE_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 2.9 WHERE p = 4", FLOAT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = '205.204.196.65' WHERE p = 5", INET_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 27429638 WHERE p = 6", INT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 31 WHERE p = 7", SMALLINT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 116 WHERE p = 8", TINYINT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 'State of Michigan' WHERE p = 9", TEXT_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = '00:20:26' WHERE p = 10", TIME_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = '2009-07-16T00:00:00' WHERE p = 11", TIMESTAMP_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = e37394dc-d17b-11e8-a8d5-f2801f1b9fd1 WHERE p = 12", UUID_COLUMN));
        executeLocal(tester, String.format("UPDATE %%s SET %s = 1fc81a4c-d17d-11e8-a8d5-f2801f1b9fd1 WHERE p = 13", TIMEUUID_COLUMN));
    }

    public void deleteCells(Executor tester) throws Throwable
    {
        for (int i = 0; i < NORMAL_COLUMNS.size(); i++)
        {
            executeLocal(tester, String.format("DELETE %s FROM %%s WHERE p = %s", NORMAL_COLUMNS.get(i).left, i));
        }
    }

    public void deleteRows(Executor tester) throws Throwable
    {
        String template = "DELETE FROM %%s WHERE p = %d";

        for (int deleted : deletable())
        {
            executeLocal(tester, String.format(template, deleted));
        }
    }

    public void executeLocal(Executor tester, String query, Object... values) throws Throwable
    {
        tester.executeLocal(formatIndexedQuery(query), values);
        tester.executeLocal(formatNonIndexedQuery(query), values);
    }

    public void executeLocalIndexed(Executor tester, String query, Object... values) throws Throwable
    {
        tester.executeLocal(formatIndexedQuery(query), values);
    }

    public List<Object> executeIndexed(Executor tester, String query, int fetchSize, Object... values)
    {
        return tester.executeRemote(formatIndexedQuery(query), fetchSize, values);
    }

    public List<Object> executeNonIndexed(Executor tester, String query, int fetchSize, Object... values)
    {
        return tester.executeRemote(formatNonIndexedQuery(query), fetchSize, values);
    }

    protected Set<Integer> deletable()
    {
        return Sets.newHashSet(3, 7, 9, 12);
    }

    private String formatIndexedQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + indexedTable);
    }

    private String formatNonIndexedQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + nonIndexedTable);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("primaryKey", primaryKey).toString();
    }

    static class CompoundKeyWithStaticsDataModel extends CompoundKeyDataModel
    {
        public CompoundKeyWithStaticsDataModel(List<Pair<String, String>> columns, List<String> rows)
        {
            super(columns, rows);

            this.keys = new CompoundPrimaryKeyList(rows.size(), 2);
        }

        @Override
        public void insertRows(Executor tester) throws Throwable
        {
            super.insertRows(tester);

            executeLocal(tester, String.format("INSERT INTO %%s (p, %s) VALUES(100, 2019)", STATIC_INT_COLUMN)); // static only
        }

        @Override
        public void updateCells(Executor tester) throws Throwable
        {
            executeLocal(tester, String.format("UPDATE %%s SET %s = 9700000000 WHERE p = 0 AND c = 0", BIGINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = false WHERE p = 0 AND c = 1", BOOLEAN_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2018-03-10' WHERE p = 1 AND c = 0", DATE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 8788.06 WHERE p = 1 AND c = 1", DOUBLE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 2.9 WHERE p = 2 AND c = 0", FLOAT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '205.204.196.65' WHERE p = 2 AND c = 1", INET_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 27429638 WHERE p = 3 AND c = 0", INT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 31 WHERE p = 3 AND c = 1", SMALLINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 116 WHERE p = 4 AND c = 0", TINYINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 'State of Michigan' WHERE p = 4 AND c = 1", TEXT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '00:20:26' WHERE p = 5 AND c = 0", TIME_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2009-07-16T00:00:00' WHERE p = 5 AND c = 1", TIMESTAMP_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = e37394dc-d17b-11e8-a8d5-f2801f1b9fd1 WHERE p = 6 AND c = 0", UUID_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 1fc81a4c-d17d-11e8-a8d5-f2801f1b9fd1 WHERE p = 6 AND c = 1", TIMEUUID_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 1896 WHERE p = 7", STATIC_INT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 2020 WHERE p = 100", STATIC_INT_COLUMN)); // static only
        }

        @Override
        public void deleteCells(Executor tester) throws Throwable
        {
            for (int i = 0; i < NORMAL_COLUMNS.size(); i++)
            {
                String[] primaryKey = keys.get(i).split(",");
                executeLocal(tester, String.format("DELETE %s FROM %%s WHERE p = %s AND c = %s", NORMAL_COLUMNS.get(i).left, primaryKey[0], primaryKey[1]));
            }
        }

        @Override
        public void deleteRows(Executor tester) throws Throwable
        {
            executeLocal(tester, "DELETE FROM %s WHERE p = 2 AND c = 0");
            executeLocal(tester, "DELETE FROM %s WHERE p = 4 AND c = 0");
            executeLocal(tester, "DELETE FROM %s WHERE p = 6");
        }

        @Override
        protected Set<Integer> deletable()
        {
            return Sets.newHashSet(4, 8, 12, 13, 100);
        }
    }

    static class CompositePartitionKeyDataModel extends BaseDataModel
    {
        public CompositePartitionKeyDataModel(List<Pair<String, String>> columns, List<String> rows)
        {
            super(columns, rows);

            this.keyColumns = ImmutableList.of(Pair.create("p1", "int"), Pair.create("p2", "int"));
            this.primaryKey = keyColumns.stream().map(pair -> pair.left).collect(Collectors.joining(", "));
            this.keys = new CompoundPrimaryKeyList(rows.size(), 2);
        }

        @Override
        public void createTables(Executor tester)
        {
            String keyColumnDefs = keyColumns.stream().map(column -> column.left + ' ' + column.right).collect(Collectors.joining(", "));
            String normalColumnDefs = columns.stream().map(column -> column.left + ' ' + column.right).collect(Collectors.joining(", "));

            String template = "CREATE TABLE %s (%s, %s, PRIMARY KEY ((%s)))" + tableOptions;
            tester.createTable(String.format(template, KEYSPACE + '.' + indexedTable, keyColumnDefs, normalColumnDefs, primaryKey));
            tester.createTable(String.format(template, KEYSPACE + '.' + nonIndexedTable, keyColumnDefs, normalColumnDefs, primaryKey));
        }

        @Override
        public void createIndexes(Executor tester) throws Throwable
        {
            super.createIndexes(tester);
            createIndexes(tester, keyColumns);
        }

        @Override
        public void insertRows(Executor tester) throws Throwable
        {
            super.insertRows(tester);
        }

        @Override
        public void updateCells(Executor tester) throws Throwable
        {
            executeLocal(tester, String.format("UPDATE %%s SET %s = 9700000000 WHERE p1 = 0 AND p2 = 0", BIGINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = false WHERE p1 = 0 AND p2 = 1", BOOLEAN_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2018-03-10' WHERE p1 = 1 AND p2 = 0", DATE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 8788.06 WHERE p1 = 1 AND p2 = 1", DOUBLE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 2.9 WHERE p1 = 2 AND p2 = 0", FLOAT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '205.204.196.65' WHERE p1 = 2 AND p2 = 1", INET_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 27429638 WHERE p1 = 3 AND p2 = 0", INT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 31 WHERE p1 = 3 AND p2 = 1", SMALLINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 116 WHERE p1 = 4 AND p2 = 0", TINYINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 'State of Michigan' WHERE p1 = 4 AND p2 = 2", TEXT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '00:20:26' WHERE p1 = 5 AND p2 = 3", TIME_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2009-07-16T00:00:00' WHERE p1 = 5 AND p2 = 1", TIMESTAMP_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = e37394dc-d17b-11e8-a8d5-f2801f1b9fd1 WHERE p1 = 6 AND p2 = 0", UUID_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 1fc81a4c-d17d-11e8-a8d5-f2801f1b9fd1 WHERE p1 = 6 AND p2 = 1", TIMEUUID_COLUMN));
        }

        @Override
        public void deleteCells(Executor tester) throws Throwable
        {
            for (int i = 0; i < NORMAL_COLUMNS.size(); i++)
            {
                String[] primaryKey = keys.get(i).split(",");
                executeLocal(tester, String.format("DELETE %s FROM %%s WHERE p1 = %s AND p2 = %s",
                                                   NORMAL_COLUMNS.get(i).left, primaryKey[0], primaryKey[1]));
            }
        }

        @Override
        public void deleteRows(Executor tester) throws Throwable
        {
            executeLocal(tester, "DELETE FROM %s WHERE p1 = 2 AND p2 = 0");
            executeLocal(tester, "DELETE FROM %s WHERE p1 = 4 AND p2 = 1");
            executeLocal(tester, "DELETE FROM %s WHERE p1 = 6 AND p2 = 2");
            executeLocal(tester, "DELETE FROM %s WHERE p1 = 8 AND p2 = 0");
        }

        @Override
        protected Set<Integer> deletable()
        {
            // already overwrites {@code deleteRows()}
            return Collections.emptySet();
        }
    }

    static class CompoundKeyDataModel extends BaseDataModel
    {
        public CompoundKeyDataModel(List<Pair<String, String>> columns, List<String> rows)
        {
            super(columns, rows);

            this.keyColumns = ImmutableList.of(Pair.create("p", "int"), Pair.create("c", "int"));
            this.primaryKey = keyColumns.stream().map(pair -> pair.left).collect(Collectors.joining(", "));
            this.keys = new CompoundPrimaryKeyList(rows.size(), 1);
        }

        @Override
        public void updateCells(Executor tester) throws Throwable
        {
            executeLocal(tester, String.format("UPDATE %%s SET %s = 9700000000 WHERE p = 0 AND c = 0", BIGINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = false WHERE p = 1 AND c = 0", BOOLEAN_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2018-03-10' WHERE p = 2 AND c = 0", DATE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 8788.06 WHERE p = 3 AND c = 0", DOUBLE_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 2.9 WHERE p = 4 AND c = 0", FLOAT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '205.204.196.65' WHERE p = 5 AND c = 0", INET_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 27429638 WHERE p = 6 AND c = 0", INT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 31 WHERE p = 7 AND c = 0", SMALLINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 116 WHERE p = 8 AND c = 0", TINYINT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 'State of Michigan' WHERE p = 9 AND c = 0", TEXT_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '00:20:26' WHERE p = 10 AND c = 0", TIME_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = '2009-07-16T00:00:00' WHERE p = 11 AND c = 0", TIMESTAMP_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = e37394dc-d17b-11e8-a8d5-f2801f1b9fd1 WHERE p = 12 AND c = 0", UUID_COLUMN));
            executeLocal(tester, String.format("UPDATE %%s SET %s = 1fc81a4c-d17d-11e8-a8d5-f2801f1b9fd1 WHERE p = 13 AND c = 0", TIMEUUID_COLUMN));
        }

        @Override
        public void deleteCells(Executor tester) throws Throwable
        {
            for (int i = 0; i < NORMAL_COLUMNS.size(); i++)
            {
                executeLocal(tester, String.format("DELETE %s FROM %%s WHERE p = %s AND c = 0", NORMAL_COLUMNS.get(i).left, i));
            }
        }
    }

    static class SimplePrimaryKeyList extends ForwardingList<String>
    {
        private final List<String> primaryKeys;

        SimplePrimaryKeyList(int rows)
        {
            this.primaryKeys = IntStream.range(0, rows).mapToObj(String::valueOf).collect(Collectors.toList());
        }

        @Override
        protected List<String> delegate()
        {
            return primaryKeys;
        }

        @Override
        public String toString()
        {
            return String.format("SimplePrimaryKeyList[rows: %d]", primaryKeys.size());
        }
    }

    static class CompoundPrimaryKeyList extends ForwardingList<String>
    {
        private final List<String> primaryKeys;
        private final int rowsPerPartition;

        CompoundPrimaryKeyList(int rows, int rowsPerPartition)
        {
            this.primaryKeys = IntStream.range(0, rows).mapToObj(v -> v / rowsPerPartition + ", " + v % rowsPerPartition).collect(Collectors.toList());
            this.rowsPerPartition = rowsPerPartition;
        }

        @Override
        protected List<String> delegate()
        {
            return primaryKeys;
        }

        @Override
        public String toString()
        {
            return String.format("CompoundPrimaryKeyList[rows: %d, partition size: %d]", primaryKeys.size(), rowsPerPartition);
        }
    }

    interface Executor
    {
        void createTable(String statement);

        void flush(String keyspace, String table);

        void compact(String keyspace, String table);

        void disableCompaction(String keyspace, String table);

        void waitForIndexQueryable(String keyspace, String index);

        void executeLocal(String query, Object...values) throws Throwable;

        List<Object> executeRemote(String query, int fetchSize, Object...values);

        void counterReset();

        long getCounter();
    }
}
