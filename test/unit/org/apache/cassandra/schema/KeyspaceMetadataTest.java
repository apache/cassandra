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

package org.apache.cassandra.schema;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableCollection;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.psjava.util.AssertStatus.assertTrue;

public class KeyspaceMetadataTest extends CQLTester
{
    private static final String NEW_KEYSPACE = "new_keyspace";

    @Test
    public void testRenameWithNestedTypes()
    {
        String type1 = createType("CREATE TYPE %s (a int, b int)");
        String type2 = createType("CREATE TYPE %s (x frozen<set<" + type1 + ">>, b int)");

        createTable("CREATE TABLE %s (pk int," +
                    " c frozen<list<" + type1 + ">>," +
                    " s frozen<map<" + type1 + "," +  type2 + ">> static," +
                    " v " + type2 + "," +
                    " v2 frozen<tuple<int, " + type1 + ">>," +
                    " v3 frozen<set<" + type1 + ">>, PRIMARY KEY (pk, c))");

        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(KEYSPACE);

        checkKeyspaceRenaming(ksMetadata, NEW_KEYSPACE);
    }

    @Test
    public void testRenameWithFunctions() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (a int, b int)");

        String function1 = createFunction(KEYSPACE, "set<frozen<" + type1 + ">>," + type1, "CREATE FUNCTION %s (state set<frozen<" + type1 + ">>, val " + type1 + ") CALLED ON NULL INPUT RETURNS set<frozen<" + type1 + ">> LANGUAGE java AS ' state = state == null ? new HashSet<>() : state; state.add(val); return state;'");
        String function2 = createFunction(KEYSPACE, "set<frozen<" + type1 + ">>", "CREATE FUNCTION %s (state set<frozen<" + type1 + ">>) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS ' return state == null ? 0 : state.size();'");
        createAggregate(KEYSPACE, type1, "CREATE AGGREGATE %s (" + type1 + ") SFUNC " + StringUtils.substringAfter(function1, ".") + " STYPE set<frozen<" + type1 + ">> FINALFUNC " + StringUtils.substringAfter(function2, "."));

        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(KEYSPACE);

        checkKeyspaceRenaming(ksMetadata, NEW_KEYSPACE);
    }

    @Test
    public void testRenameWithDroppedColumns() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (a int, b int)");
        String type2 = createType("CREATE TYPE %s (x frozen<set<" + type1 + ">>, b int)");

        createTable("CREATE TABLE %s (pk int, c frozen<" + type1 + ">, s int static, v " + type2 + ", v2 text, PRIMARY KEY (pk, c))");
        execute("ALTER TABLE %s DROP v2");
        execute("ALTER TABLE %s DROP s");

        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(KEYSPACE);

        checkKeyspaceRenaming(ksMetadata, NEW_KEYSPACE);
    }

    @Test
    public void testRenameWithIndex() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (pk int, c frozen<" + type1 + ">, s int static, v int, PRIMARY KEY (pk, c))");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (v)");

        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(KEYSPACE);

        checkKeyspaceRenaming(ksMetadata, NEW_KEYSPACE);
    }

    @Test
    public void testRenameWithMv() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (a int, b int)");

        createTable("CREATE TABLE %s (pk int, c frozen<" + type1 + ">, v int, v2 text, PRIMARY KEY (pk, c))");
        execute("CREATE MATERIALIZED VIEW " + KEYSPACE + ".mv AS SELECT c, v, pk FROM " + currentTable() + " WHERE v IS NOT NULL AND c IS NOT NULL AND pk IS NOT NULL PRIMARY KEY (v, pk, c);");

        KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(KEYSPACE);

        checkKeyspaceRenaming(ksMetadata, NEW_KEYSPACE);
    }

    private void checkKeyspaceRenaming(KeyspaceMetadata original, String newName)
    {
        KeyspaceMetadata renamed = original.rename(newName);
        assertEquals(newName, renamed.name);
        assertEquals(original.kind, renamed.kind);
        assertEquals(original.params, renamed.params);

        original.types.forEach(t -> checkKeyspaceRenamingForType(newName, renamed.types, t, renamed.types.getNullable(t.name)));
        checkKeyspaceRenamingForFunctions(newName, original, renamed);
        original.tables.forEach(t -> checkKeyspaceRenamingForTable(newName, renamed.types, t, renamed.tables.getNullable(t.name)));
        original.views.forEach(v -> checkKeyspaceRenamingForView(newName, renamed.types, v, renamed.views.getNullable(v.name())));
    }

    private void checkKeyspaceRenamingForFunctions(String newName, KeyspaceMetadata original, KeyspaceMetadata renamed)
    {
        Set<FunctionName> names = original.userFunctions.stream().map(Function::name).collect(Collectors.toSet());
        for (FunctionName name : names)
        {
            Iterator<UserFunction> originalIter = original.userFunctions.get(name).iterator();
            Iterator<UserFunction> updatedIter = renamed.userFunctions.get(new FunctionName(newName, name.name)).iterator();
            while (originalIter.hasNext() && updatedIter.hasNext())
            {
                checkKeyspaceRenamingForFunction(newName, renamed.userFunctions, renamed.types, originalIter.next(), updatedIter.next());
            }
            assertFalse(originalIter.hasNext());
            assertFalse(updatedIter.hasNext());
        }
    }

    private void checkKeyspaceRenamingForType(String keyspace,
                                              Types newTypes,
                                              AbstractType<?> originalType,
                                              AbstractType<?> updatedType)
    {
        if (originalType.isUDT())
        {
            assertTrue(updatedType.isUDT());
            UserType updatedUserType = (UserType) updatedType;
            UserType originalUserType = (UserType) originalType;

            // Checks that the updated type is present in the updated Types
            UserType tmp = newTypes.getNullable(updatedUserType.name);
            tmp = updatedType.isMultiCell() ? tmp : tmp.freeze();
            assertEquals(updatedType, tmp);

            assertEquals(keyspace, updatedUserType.keyspace);
            assertEquals(originalUserType.name, updatedUserType.name);
            assertEquals(originalUserType.fieldNames(), updatedUserType.fieldNames());
        }
        else if (originalType.referencesUserTypes())
        {
            List<AbstractType<?>> originalSubTypes = originalType.subTypes();
            List<AbstractType<?>> updatedSubTypes = updatedType.subTypes();

            assertEquals(originalSubTypes.size(), updatedSubTypes.size());
            for (int i = 0, m = originalSubTypes.size(); i < m; i++)
            {
                checkKeyspaceRenamingForType(keyspace, newTypes, originalSubTypes.get(i), updatedSubTypes.get(i));
            }
        }
        else
        {
            assertEquals(originalType, updatedType);
        }
    }

    private void checkKeyspaceRenamingForFunction(String keyspace,
                                                  UserFunctions newFunctions,
                                                  Types newTypes,
                                                  Function originalFunction,
                                                  Function updatedFunction)
    {
        if (originalFunction.isNative())
        {
            assertEquals(originalFunction, updatedFunction);
        }
        else
        {
            // Checks that the updated function is present in the updated Types
            Optional<UserFunction> maybe = newFunctions.find(updatedFunction.name(), updatedFunction.argTypes());
            assertTrue(maybe.isPresent());
            assertEquals(updatedFunction, maybe.get());

            assertEquals(keyspace, updatedFunction.name().keyspace);
            assertNotEquals(originalFunction.name().keyspace, updatedFunction.name().keyspace);
            assertEquals(originalFunction.name().name, updatedFunction.name().name);
            assertEquals(originalFunction.isAggregate(), updatedFunction.isAggregate());

            List<AbstractType<?>> originalArgTypes = originalFunction.argTypes();
            List<AbstractType<?>> updatedArgTypes = updatedFunction.argTypes();
            assertEquals(originalArgTypes.size(), updatedArgTypes.size());
            for (int i = 0, m = originalArgTypes.size(); i < m; i++)
            {
                checkKeyspaceRenamingForType(keyspace, newTypes, originalArgTypes.get(i), updatedArgTypes.get(i));
            }

            if (originalFunction.isAggregate())
            {
                UDAggregate originalUa = (UDAggregate) originalFunction;
                UDAggregate updatedUa = (UDAggregate) updatedFunction;

                checkKeyspaceRenamingForFunction(keyspace,
                                                 newFunctions,
                                                 newTypes,
                                                 originalUa.finalFunction(),
                                                 updatedUa.finalFunction());
                assertEquals(originalUa.initialCondition(), updatedUa.initialCondition());
                checkKeyspaceRenamingForFunction(keyspace,
                                                 newFunctions,
                                                 newTypes,
                                                 originalUa.stateFunction(),
                                                 updatedUa.stateFunction());
                checkKeyspaceRenamingForType(keyspace, newTypes, originalUa.stateType(), updatedUa.stateType());
            }
            else
            {
                UDFunction originalUdf = (UDFunction) originalFunction;
                UDFunction updatedUdf = (UDFunction) updatedFunction;

                assertEquals(originalUdf.language(), updatedUdf.language());
                assertEquals(originalUdf.body(), updatedUdf.body());
                assertEquals(originalUdf.isCalledOnNullInput(), updatedUdf.isCalledOnNullInput());
            }
        }
    }

    private void checkKeyspaceRenamingForTable(String keyspace,
                                               Types updatedTypes,
                                               TableMetadata originalTable,
                                               TableMetadata updatedTable)
    {
        assertEquals(keyspace, updatedTable.keyspace);
        assertNotEquals(originalTable.keyspace, updatedTable.keyspace);
        assertNotEquals(originalTable.id, updatedTable.id);
        assertEquals(originalTable.name, updatedTable.name);
        assertEquals(originalTable.partitioner, updatedTable.partitioner);
        assertEquals(originalTable.kind, updatedTable.kind);
        assertEquals(originalTable.flags, updatedTable.flags);
        assertEquals(originalTable.indexes, updatedTable.indexes);
        assertEquals(originalTable.triggers, updatedTable.triggers);
        assertEquals(originalTable.params, updatedTable.params);

        originalTable.columns().forEach(c -> checkKeyspaceRenamingForColumn(keyspace,
                                                                            updatedTypes,
                                                                            c,
                                                                            updatedTable.getColumn(c.name.bytes)));
        ImmutableCollection<DroppedColumn> droppedColumns = originalTable.droppedColumns.values();
        droppedColumns.forEach(c -> checkKeyspaceRenamingForColumn(keyspace,
                                                                   updatedTypes,
                                                                   c.column,
                                                                   updatedTable.getDroppedColumn(c.column.name.bytes)));
    }

    private void checkKeyspaceRenamingForView(String keyspace,
                                              Types updatedTypes,
                                              ViewMetadata originalView,
                                              ViewMetadata updatedView)
    {
        checkKeyspaceRenamingForTable(keyspace, updatedTypes, originalView.metadata, updatedView.metadata);

        Iterator<ColumnMetadata> requiredOriginal = originalView.metadata.columns().iterator();
        Iterator<ColumnMetadata> requiredUpdated = updatedView.metadata.columns().iterator();

        while (requiredOriginal.hasNext() && requiredUpdated.hasNext())
        {
            checkKeyspaceRenamingForColumn(keyspace,
                                           updatedTypes,
                                           requiredOriginal.next(),
                                           requiredUpdated.next());
        }
        assertFalse(requiredOriginal.hasNext());
        assertFalse(requiredUpdated.hasNext());
    }

    private void checkKeyspaceRenamingForColumn(String keyspace,
                                                Types updatedTypes,
                                                ColumnMetadata originalColumn,
                                                ColumnMetadata updatedColumn)
    {
        assertEquals(keyspace, updatedColumn.ksName);
        assertNotEquals(originalColumn.ksName, updatedColumn.ksName);
        assertEquals(originalColumn.cfName, updatedColumn.cfName);
        assertEquals(originalColumn.name, updatedColumn.name);
        checkKeyspaceRenamingForType(keyspace, updatedTypes, originalColumn.type, updatedColumn.type);
    }
}
