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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.cql3.restrictions.TermSlice;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.cql3.statements.SelectStatement.getComponents;

/**
 * Class incapsulating the helper logic to handle SELECT / UPDATE / INSERT special-cases related
 * to SuperColumn tables in applicable scenarios.
 *
 * SuperColumn families have a special layout and are represented as a Map internally. These tables
 * have two special columns (called `column2` and `value` by default):
 *
 *   * `column2`, {@link CFMetaData#superCfValueColumn}, a key of the SuperColumn map, exposed as a
 *   REGULAR column, but stored in schema tables as a CLUSTERING column to make a distinction from
 *   the SC value column in case of renames.
 *   * `value`, {@link CFMetaData#compactValueColumn()}, a value of the SuperColumn map, exposed and
 *   stored as a REGULAR column
 *
 * These columns have to be translated to this internal representation as key and value, correspondingly.
 *
 * In CQL terms, the SuperColumn families is encoded with:
 *
 *   CREATE TABLE super (
 *      key [key_validation_class],
 *      super_column_name [comparator],
 *      [column_metadata_1] [type1],
 *      ...,
 *      [column_metadata_n] [type1],
 *      "" map<[sub_comparator], [default_validation_class]>
 *      PRIMARY KEY (key, super_column_name)
 *   )
 *
 * In other words, every super column is encoded by a row. That row has one column for each defined
 * "column_metadata", but it also has a special map column (whose name is the empty string as this is
 * guaranteed to never conflict with a user-defined "column_metadata") which stores the super column
 * "dynamic" sub-columns.
 *
 * On write path, `column2` and `value` columns are translated to the key and value of the
 * underlying map. During the read, the inverse conversion is done. Deletes are converted into
 * discards by the key in the underlying map. Counters are handled by translating an update to a
 * counter update with a cell path. See {@link SuperColumnRestrictions} for the details.
 *
 * Since non-dense SuperColumn families do not modify the contents of the internal map through in CQL
 * and do not expose this via CQL either, reads, writes and deletes are handled normally.
 *
 * Sidenote: a _dense_ SuperColumn Familiy is the one that has no added REGULAR columns.
 */
public class SuperColumnCompatibility
{
    // We use an empty value for the 1) this can't conflict with a user-defined column and 2) this actually
    // validate with any comparator which makes it convenient for columnDefinitionComparator().
    public static final ByteBuffer SUPER_COLUMN_MAP_COLUMN = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    public static final String SUPER_COLUMN_MAP_COLUMN_STR = UTF8Type.instance.compose(SUPER_COLUMN_MAP_COLUMN);

    /**
     * Dense flag might have been incorrectly set if the node was upgraded from 2.x before CASSANDRA-12373.
     *
     * For 3.x created tables, the flag is set correctly in ThriftConversion code.
     */
    public static boolean recalculateIsDense(Columns columns)
    {
        return columns.size() == 1 && columns.getComplex(0).name.toString().isEmpty();
    }

    /**
     * For _dense_ SuperColumn Families, the supercolumn key column has to be translated to the collection subselection
     * query in order to avoid reading an entire collection and then filtering out the results.
     */
    public static ColumnFilter getColumnFilter(CFMetaData cfm, QueryOptions queryOptions, SuperColumnRestrictions restrictions)
    {
        assert cfm.isSuper() && cfm.isDense();

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.add(cfm.compactValueColumn());

        if (restrictions.keySliceRestriction != null)
        {
            SingleColumnRestriction.SuperColumnKeySliceRestriction restriction = restrictions.keySliceRestriction;
            TermSlice slice = restriction.slice;

            ByteBuffer start = slice.hasBound(Bound.START) ? slice.bound(Bound.START).bindAndGet(queryOptions) : null;
            ByteBuffer end = slice.hasBound(Bound.END) ? slice.bound(Bound.END).bindAndGet(queryOptions) : null;

            builder.slice(cfm.compactValueColumn(),
                          start == null ? CellPath.BOTTOM : CellPath.create(start),
                          end == null ? CellPath.TOP : CellPath.create(end));
        }
        else if (restrictions.keyEQRestriction != null)
        {
            SingleColumnRestriction.SuperColumnKeyEQRestriction restriction = restrictions.keyEQRestriction;
            ByteBuffer value = restriction.bindValue(queryOptions);
            builder.select(cfm.compactValueColumn(), CellPath.create(value));
        }
        else if (restrictions.keyINRestriction != null)
        {
            SingleColumnRestriction.SuperColumnKeyINRestriction cast = restrictions.keyINRestriction;
            Set<ByteBuffer> keyINRestrictionValues = new TreeSet<ByteBuffer>(((MapType) cfm.compactValueColumn().type).getKeysType());
            keyINRestrictionValues.addAll(cast.getValues(queryOptions));

            for (ByteBuffer value : keyINRestrictionValues)
                builder.select(cfm.compactValueColumn(), CellPath.create(value));
        }
        else if (restrictions.multiEQRestriction != null)
        {
            SingleColumnRestriction.SuperColumnMultiEQRestriction restriction = restrictions.multiEQRestriction;
            ByteBuffer value = restriction.secondValue;
            builder.select(cfm.compactValueColumn(), CellPath.create(value));
        }

        return builder.build();
    }

    /**
     * For _dense_ SuperColumn Families.
     *
     * On read path, instead of writing row per map, we have to write a row per key/value pair in map.
     *
     * For example:
     *
     *   | partition-key | clustering-key | { key1: value1, key2: value2 } |
     *
     * Will be translated to:
     *
     *   | partition-key | clustering-key | key1 | value1 |
     *   | partition-key | clustering-key | key2 | value2 |
     *
     */
    public static void processPartition(CFMetaData cfm, Selection selection, RowIterator partition, Selection.ResultSetBuilder result, ProtocolVersion protocolVersion,
                                        SuperColumnRestrictions restrictions, QueryOptions queryOptions)
    {
        assert cfm.isDense();
        ByteBuffer[] keyComponents = getComponents(cfm, partition.partitionKey());

        int nowInSeconds = FBUtilities.nowInSeconds();
        while (partition.hasNext())
        {
            Row row = partition.next();

            ComplexColumnData ccd = row.getComplexColumnData(cfm.compactValueColumn());

            if (ccd == null)
                continue;

            Iterator<Cell> cellIter = ccd.iterator();

            outer:
            while (cellIter.hasNext())
            {
                Cell cell = cellIter.next();
                ByteBuffer superColumnKey = cell.path().get(0);

                if (restrictions != null)
                {
                    // Slice on SuperColumn key
                    if (restrictions.keySliceRestriction != null)
                    {
                        for (Bound bound : Bound.values())
                        {
                            if (restrictions.keySliceRestriction.hasBound(bound) &&
                                !restrictions.keySliceRestriction.isInclusive(bound))
                            {
                                ByteBuffer excludedValue = restrictions.keySliceRestriction.bindValue(queryOptions);
                                if (excludedValue.equals(superColumnKey))
                                    continue outer;
                            }
                        }
                    }

                    // Multi-column restriction on clustering+SuperColumn key
                    if (restrictions.multiSliceRestriction != null &&
                        cfm.comparator.compare(row.clustering(), Clustering.make(restrictions.multiSliceRestriction.firstValue)) == 0)
                    {
                        AbstractType t = ((MapType) cfm.compactValueColumn().type).getKeysType();
                        int cmp = t.compare(superColumnKey, restrictions.multiSliceRestriction.secondValue);

                        if ((cmp == 0 && !restrictions.multiSliceRestriction.trueInclusive) ||     // EQ
                            (restrictions.multiSliceRestriction.hasBound(Bound.END) && cmp > 0) || // LT
                            (restrictions.multiSliceRestriction.hasBound(Bound.START) && cmp < 0)) // GT
                            continue outer;
                    }
                }

                Row staticRow = partition.staticRow();
                result.newRow(partition.partitionKey(), staticRow.clustering());

                for (ColumnDefinition def : selection.getColumns())
                {
                    if (cfm.isSuperColumnKeyColumn(def))
                    {
                        result.add(superColumnKey);
                    }
                    else if (cfm.isSuperColumnValueColumn(def))
                    {
                        result.add(cell, nowInSeconds);
                    }
                    else
                    {
                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                                result.add(keyComponents[def.position()]);
                                break;
                            case CLUSTERING:
                                result.add(row.clustering().get(def.position()));
                                break;
                            case REGULAR:
                            case STATIC:
                                throw new AssertionError(String.format("Invalid column '%s' found in SuperColumn table", def.name.toString()));
                        }
                    }
                }
            }
        }
    }

    /**
     * For _dense_ SuperColumn Families.
     *
     * On the write path, we have to do combine the columns into a key/value pair:
     *
     * So inserting a row:
     *
     *     | partition-key | clustering-key | key1 | value1 |
     *
     * Would result into:
     *
     *     | partition-key | clustering-key | {key1: value1} |
     *
     * or adding / overwriting the value for `key1`.
     */
    public static void prepareInsertOperations(CFMetaData cfm,
                                               List<ColumnDefinition.Raw> columnNames,
                                               WhereClause.Builder whereClause,
                                               List<Term.Raw> columnValues,
                                               VariableSpecifications boundNames,
                                               Operations operations)
    {
        List<ColumnDefinition> defs = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++)
        {
            ColumnDefinition id = columnNames.get(i).prepare(cfm);
            defs.add(id);
        }

        prepareInsertOperations(cfm, defs, boundNames, columnValues, whereClause, operations);
    }

    /**
     * For _dense_ SuperColumn Families.
     *
     * {@link #prepareInsertOperations(CFMetaData, List, VariableSpecifications, List, WhereClause.Builder, Operations)},
     * but for INSERT JSON queries
     */
    public static void prepareInsertJSONOperations(CFMetaData cfm,
                                                   List<ColumnDefinition> defs,
                                                   VariableSpecifications boundNames,
                                                   Json.Prepared prepared,
                                                   WhereClause.Builder whereClause,
                                                   Operations operations)
    {
        List<Term.Raw> columnValues = new ArrayList<>(defs.size());
        for (ColumnDefinition def : defs)
            columnValues.add(prepared.getRawTermForColumn(def, true));

        prepareInsertOperations(cfm, defs, boundNames, columnValues, whereClause, operations);
    }

    private static void prepareInsertOperations(CFMetaData cfm,
                                                List<ColumnDefinition> defs,
                                                VariableSpecifications boundNames,
                                                List<Term.Raw> columnValues,
                                                WhereClause.Builder whereClause,
                                                Operations operations)
    {
        assert cfm.isDense();
        assert defs.size() == columnValues.size();

        Term.Raw superColumnKey = null;
        Term.Raw superColumnValue = null;

        for (int i = 0, size = defs.size(); i < size; i++)
        {
            ColumnDefinition def = defs.get(i);
            Term.Raw raw = columnValues.get(i);

            if (cfm.isSuperColumnKeyColumn(def))
            {
                superColumnKey = raw;
                collectMarkerSpecifications(raw, boundNames, def);
            }
            else if (cfm.isSuperColumnValueColumn(def))
            {
                superColumnValue = raw;
                collectMarkerSpecifications(raw, boundNames, def);
            }
            else if (def.isPrimaryKeyColumn())
            {
                whereClause.add(new SingleColumnRelation(ColumnDefinition.Raw.forColumn(def), Operator.EQ, raw));
            }
            else
            {
                throw invalidRequest("Invalid column {} in where clause");
            }
        }

        checkTrue(superColumnValue != null,
                  "Column value is mandatory for SuperColumn tables");
        checkTrue(superColumnKey != null,
                  "Column key is mandatory for SuperColumn tables");

        Operation operation = new Operation.SetElement(superColumnKey, superColumnValue).prepare(cfm, cfm.compactValueColumn());
        operations.add(operation);
    }

    /**
     * Collect the marker specifications for the bound columns manually, since the operations on a column are
     * converted to the operations on the collection element.
     */
    private static void collectMarkerSpecifications(Term.Raw raw, VariableSpecifications boundNames, ColumnDefinition def)
    {
        if (raw instanceof AbstractMarker.Raw)
            boundNames.add(((AbstractMarker.Raw) raw).bindIndex(), def);
    }

    /**
     * For _dense_ SuperColumn Families.
     *
     * During UPDATE operation, the update by clustering (with correponding relation in WHERE clause)
     * has to be substituted with an update to the map that backs the given SuperColumn.
     *
     * For example, an update such as:
     *
     *     UPDATE ... SET value = 'value1' WHERE key = 'pk' AND column1 = 'ck' AND column2 = 'mk'
     *
     * Will update the value under key 'mk' in the map, backing the SuperColumn, located in the row
     * with clustering 'ck' in the partition with key 'pk'.
     */
    public static WhereClause prepareUpdateOperations(CFMetaData cfm,
                                                      WhereClause whereClause,
                                                      List<Pair<ColumnDefinition.Raw, Operation.RawUpdate>> updates,
                                                      VariableSpecifications boundNames,
                                                      Operations operations)
    {
        assert cfm.isDense();
        Term.Raw superColumnKey = null;
        Term.Raw superColumnValue = null;

        List<Relation> newRelations = new ArrayList<>(whereClause.relations.size());
        for (int i = 0; i < whereClause.relations.size(); i++)
        {
            SingleColumnRelation relation = (SingleColumnRelation) whereClause.relations.get(i);
            ColumnDefinition def = relation.getEntity().prepare(cfm);

            if (cfm.isSuperColumnKeyColumn(def))
            {
                superColumnKey = relation.getValue();
                collectMarkerSpecifications(superColumnKey, boundNames, def);
            }
            else
            {
                newRelations.add(relation);
            }
        }

        checkTrue(superColumnKey != null,
                  "Column key is mandatory for SuperColumn tables");

        for (Pair<ColumnDefinition.Raw, Operation.RawUpdate> entry : updates)
        {
            ColumnDefinition def = entry.left.prepare(cfm);

            if (!cfm.isSuperColumnValueColumn(def))
                throw invalidRequest("Column `%s` of type `%s` found in SET part", def.name, def.type.asCQL3Type());

            Operation operation;

            if (entry.right instanceof Operation.Addition)
            {
                Operation.Addition op = (Operation.Addition) entry.right;
                superColumnValue = op.value();

                operation = new Operation.ElementAddition(superColumnKey, superColumnValue).prepare(cfm, cfm.compactValueColumn());
            }
            else if (entry.right instanceof Operation.Substraction)
            {
                Operation.Substraction op = (Operation.Substraction) entry.right;
                superColumnValue = op.value();

                operation = new Operation.ElementSubtraction(superColumnKey, superColumnValue).prepare(cfm, cfm.compactValueColumn());
            }
            else if (entry.right instanceof Operation.SetValue)
            {
                Operation.SetValue op = (Operation.SetValue) entry.right;
                superColumnValue = op.value();

                operation = new Operation.SetElement(superColumnKey, superColumnValue).prepare(cfm, cfm.compactValueColumn());
            }
            else
            {
                throw invalidRequest("Invalid operation `%s` on column `%s` of type `%s` found in SET part", entry.right, def.name, def.type.asCQL3Type());
            }

            collectMarkerSpecifications(superColumnValue, boundNames, def);
            operations.add(operation);
        }

        checkTrue(superColumnValue != null,
                  "Column value is mandatory for SuperColumn tables");

        return newRelations.size() != whereClause.relations.size() ? whereClause.copy(newRelations) : whereClause;
    }

    /**
     * Rebuilds LWT conditions on SuperColumn _value_ column.
     *
     * Conditions have to be changed to correspond the internal representation of SuperColumn value, since it's not
     * a separate column, but a value in a hidden compact value column.
     */
    public static Conditions rebuildLWTColumnConditions(Conditions conditions, CFMetaData cfm, WhereClause whereClause)
    {
        if (conditions.isEmpty() || conditions.isIfExists() || conditions.isIfNotExists())
            return conditions;

        ColumnConditions.Builder builder = ColumnConditions.newBuilder();
        Collection<ColumnCondition> columnConditions = ((ColumnConditions) conditions).columnConditions();

        Pair<ColumnDefinition, Relation> superColumnKeyRelation = SuperColumnCompatibility.getSuperColumnKeyRelation(whereClause.relations, cfm);

        checkNotNull(superColumnKeyRelation,
                     "Lightweight transactions on SuperColumn tables are only supported with supplied SuperColumn key");

        for (ColumnCondition columnCondition : columnConditions)
        {
            checkTrue(cfm.isSuperColumnValueColumn(columnCondition.column),
                      "Lightweight transactions are only supported on the value column of SuperColumn tables");

            Term.Raw value = superColumnKeyRelation.right.getValue();
            Term collectionElemnt = value instanceof AbstractMarker.Raw ?
                                    new Constants.Marker(((AbstractMarker.Raw) value).bindIndex(),
                                                         superColumnKeyRelation.left) :
                                    value.prepare(cfm.ksName, superColumnKeyRelation.left);
            builder.add(ColumnCondition.condition(cfm.compactValueColumn(),
                                                  collectionElemnt,
                                                  columnCondition.value(), columnCondition.operator));
        }

        return builder.build();
    }

    /**
     * Returns a relation on the SuperColumn key
     */
    private static Pair<ColumnDefinition, Relation> getSuperColumnKeyRelation(List<Relation> relations, CFMetaData cfm)
    {
        for (int i = 0; i < relations.size(); i++)
        {
            SingleColumnRelation relation = (SingleColumnRelation) relations.get(i);
            ColumnDefinition def = relation.getEntity().prepare(cfm);

            if (cfm.isSuperColumnKeyColumn(def))
                return Pair.create(def, relation);
        }
        return null;
    }

    /**
     * For _dense_ SuperColumn Families.
     *
     * Delete, when the "regular" columns are present, have to be translated into
     * deletion of value in the internal map by key.
     *
     * For example, delete such as:
     *
     *     DELETE FROM ... WHERE key = 'pk' AND column1 = 'ck' AND column2 = 'mk'
     *
     * Will delete a value under 'mk' from the map, located in the row with clustering key 'ck' in the partition
     * with key 'pk'.
     */
    public static WhereClause prepareDeleteOperations(CFMetaData cfm,
                                                      WhereClause whereClause,
                                                      VariableSpecifications boundNames,
                                                      Operations operations)
    {
        assert cfm.isDense();
        List<Relation> newRelations = new ArrayList<>(whereClause.relations.size());

        for (int i = 0; i < whereClause.relations.size(); i++)
        {
            Relation orig = whereClause.relations.get(i);

            checkFalse(orig.isMultiColumn(),
                       "Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", orig);
            checkFalse(orig.onToken(),
                       "Token relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", orig);

            SingleColumnRelation relation = (SingleColumnRelation) orig;
            ColumnDefinition def = relation.getEntity().prepare(cfm);

            if (cfm.isSuperColumnKeyColumn(def))
            {
                Term.Raw value = relation.getValue();

                if (value instanceof AbstractMarker.Raw)
                    boundNames.add(((AbstractMarker.Raw) value).bindIndex(), def);

                Operation operation = new Maps.DiscarderByKey(cfm.compactValueColumn(), value.prepare(cfm.ksName, def));
                operations.add(operation);
            }
            else
            {
                newRelations.add(relation);
            }
        }

        return newRelations.size() != whereClause.relations.size() ? whereClause.copy(newRelations) : whereClause;
    }

    /**
     * Create a column name generator for SuperColumns
     */
    public static CompactTables.DefaultNames columnNameGenerator(List<ColumnDefinition> partitionKeyColumns,
                                                                 List<ColumnDefinition> clusteringColumns,
                                                                 PartitionColumns partitionColumns)
    {
        Set<String> names = new HashSet<>();
        // If the clustering column was renamed, the supercolumn key's default nname still can't be `column1` (SuperColumn
        // key renames are handled separately by looking up an existing column).
        names.add("column1");
        for (ColumnDefinition columnDefinition: partitionKeyColumns)
            names.add(columnDefinition.name.toString());
        for (ColumnDefinition columnDefinition: clusteringColumns)
            names.add(columnDefinition.name.toString());
        for (ColumnDefinition columnDefinition: partitionColumns)
            names.add(columnDefinition.name.toString());

        return CompactTables.defaultNameGenerator(names);
    }

    /**
     * Find a SuperColumn key column if it's available (for example, when it was renamed) or create one with a default name.
     */
    public static ColumnDefinition getSuperCfKeyColumn(CFMetaData cfm, List<ColumnDefinition> clusteringColumns, CompactTables.DefaultNames defaultNames)
    {
        assert cfm.isDense();

        MapType mapType = (MapType) cfm.compactValueColumn().type;
        // Pre CASSANDRA-12373 3.x-created supercolumn family
        if (clusteringColumns.size() == 1)
        {
            // create a new one with a default name
            ColumnIdentifier identifier = ColumnIdentifier.getInterned(defaultNames.defaultClusteringName(), true);
            return new ColumnDefinition(cfm.ksName, cfm.cfName, identifier, mapType.getKeysType(), ColumnDefinition.NO_POSITION, ColumnDefinition.Kind.REGULAR);
        }

        // Upgrade path: table created in 2.x, handle pre-created columns and/or renames.
        assert clusteringColumns.size() == 2 : clusteringColumns;
        ColumnDefinition cd = clusteringColumns.get(1);

        assert cd.type.equals(mapType.getKeysType()) : cd.type + " != " + mapType.getKeysType();
        return new ColumnDefinition(cfm.ksName, cfm.cfName, cd.name, mapType.getKeysType(), ColumnDefinition.NO_POSITION, ColumnDefinition.Kind.REGULAR);
    }

    /**
     * Find a SuperColumn value column if it's available (for example, when it was renamed) or create one with a default name.
     */
    public static ColumnDefinition getSuperCfValueColumn(CFMetaData cfm, PartitionColumns partitionColumns, ColumnDefinition superCfKeyColumn, CompactTables.DefaultNames defaultNames)
    {
        assert cfm.isDense();

        MapType mapType = (MapType) cfm.compactValueColumn().type;
        for (ColumnDefinition def: partitionColumns.regulars)
        {
            if (!def.name.bytes.equals(SUPER_COLUMN_MAP_COLUMN) && def.type.equals(mapType.getValuesType()) && !def.equals(superCfKeyColumn))
                return def;
        }

        ColumnIdentifier identifier = ColumnIdentifier.getInterned(defaultNames.defaultCompactValueName(), true);
        return new ColumnDefinition(cfm.ksName, cfm.cfName, identifier, mapType.getValuesType(), ColumnDefinition.NO_POSITION, ColumnDefinition.Kind.REGULAR);
    }

    /**
     * SuperColumn key is stored in {@link CFMetaData#columnMetadata} as a clustering column (to make sure we can make
     * a distinction between the SuperColumn key and SuperColumn value columns, especially when they have the same type
     * and were renamed), but exposed as {@link CFMetaData#superCfKeyColumn} as a regular column to be compatible with
     * the storage engine.
     *
     * This remapping is necessary to facilitate the column metadata part.
     */
    public static ColumnDefinition getSuperCfSschemaRepresentation(ColumnDefinition superCfKeyColumn)
    {
        return new ColumnDefinition(superCfKeyColumn.ksName, superCfKeyColumn.cfName, superCfKeyColumn.name, superCfKeyColumn.type, 1, ColumnDefinition.Kind.CLUSTERING);
    }

    public static boolean isSuperColumnMapColumn(ColumnDefinition column)
    {
        return column.isRegular() && column.name.bytes.equals(SuperColumnCompatibility.SUPER_COLUMN_MAP_COLUMN);
    }

    public static ColumnDefinition getCompactValueColumn(PartitionColumns columns)
    {
        for (ColumnDefinition column : columns.regulars)
        {
            if (isSuperColumnMapColumn(column))
                return column;
        }
        throw new AssertionError("Invalid super column table definition, no 'dynamic' map column");
    }

    /**
     * Restrictions are the trickiest part of the SuperColumn integration.
     * See specific docs on each field. For the purpose of this doc, the "default" column names are used,
     * `column2` and `value`. Detailed description and semantics of these fields can be found in this class'
     * header comment.
     */
    public static class SuperColumnRestrictions
    {
        /**
         * Restrictions in the form of:
         *   ... AND (column1, column2) > ('value1', 1)
         * Multi-column restrictions. `column1` will be handled normally by the clustering bounds,
         * and `column2` value has to be "saved" and filtered out in `processPartition`, as there's no
         * direct mapping of multi-column restrictions to clustering + cell path. The first row
         * is special-cased to make sure the semantics of multi-column restrictions are preserved.
         */
        private final SingleColumnRestriction.SuperColumnMultiSliceRestriction multiSliceRestriction;

        /**
         * Restrictions in the form of:
         *   ... AND (column1, column2) = ('value1', 1)
         * Multi-column restriction with EQ does have a direct mapping: `column1` will be handled
         * normally by the clustering bounds, and the `column2` will be special-cased by the
         * {@link #getColumnFilter(CFMetaData, QueryOptions, SuperColumnRestrictions)} as a collection path lookup.
         */
        private final SingleColumnRestriction.SuperColumnMultiEQRestriction multiEQRestriction;

        /**
         * Restrictions in the form of:
         *   ... AND column2 >= 5
         * For non-filtering cases (when the preceding clustering column and a partition key are
         * restricted), will be handled in {@link #getColumnFilter(CFMetaData, QueryOptions, SuperColumnRestrictions)}
         * like an inclusive bounds lookup.
         *
         * For the restrictions taking a form of
         *   ... AND column2 > 5
         * (non-inclusive ones), the items that match `=` will be filtered out
         * by {@link #processPartition(CFMetaData, Selection, RowIterator, Selection.ResultSetBuilder, ProtocolVersion, SuperColumnRestrictions, QueryOptions)}
         *
         * Unfortunately, there are no good ways to do it other than here:
         * {@link RowFilter} can't be used in this case, since the complex collection cells are not yet rows by that
         * point.
         * {@link ColumnFilter} (which is used for inclusive slices) can't be changed to support exclusive slices as it would
         * require a protocol change in order to add a Kind. So exclusive slices are a combination of inclusive plus
         * an ad-hoc filter.
         */
        private final SingleColumnRestriction.SuperColumnKeySliceRestriction keySliceRestriction;

        /**
         * Restrictions in the form of:
         *   ... AND column2 IN (1, 2, 3)
         * For non-filtering cases (when the preceeding clustering column and a partition key are
         * restricted), are handled in {@link #getColumnFilter(CFMetaData, QueryOptions, SuperColumnRestrictions)} by
         * adding multiple collection paths to the {@link ColumnFilter}
         */
        private final SingleColumnRestriction.SuperColumnKeyINRestriction keyINRestriction;

        /**
         * Restrictions in the form of:
         *   ... AND column2 = 1
         * For non-filtering cases (when the preceeding clustering column and a partition key are
         * restricted), will be handled by converting the restriction to the column filter on
         * the collection key in {@link #getColumnFilter(CFMetaData, QueryOptions, SuperColumnRestrictions)}
         */
        private final SingleColumnRestriction.SuperColumnKeyEQRestriction keyEQRestriction;

        public SuperColumnRestrictions(Iterator<SingleRestriction> restrictions)
        {
            // In order to keep the fields final, assignments have to be done outside the loop
            SingleColumnRestriction.SuperColumnMultiSliceRestriction multiSliceRestriction = null;
            SingleColumnRestriction.SuperColumnKeySliceRestriction keySliceRestriction = null;
            SingleColumnRestriction.SuperColumnKeyINRestriction keyINRestriction = null;
            SingleColumnRestriction.SuperColumnMultiEQRestriction multiEQRestriction = null;
            SingleColumnRestriction.SuperColumnKeyEQRestriction keyEQRestriction = null;

            while (restrictions.hasNext())
            {
                SingleRestriction restriction = restrictions.next();

                if (restriction instanceof SingleColumnRestriction.SuperColumnMultiSliceRestriction)
                    multiSliceRestriction = (SingleColumnRestriction.SuperColumnMultiSliceRestriction) restriction;
                else if (restriction instanceof SingleColumnRestriction.SuperColumnKeySliceRestriction)
                    keySliceRestriction = (SingleColumnRestriction.SuperColumnKeySliceRestriction) restriction;
                else if (restriction instanceof SingleColumnRestriction.SuperColumnKeyINRestriction)
                    keyINRestriction = (SingleColumnRestriction.SuperColumnKeyINRestriction) restriction;
                else if (restriction instanceof SingleColumnRestriction.SuperColumnMultiEQRestriction)
                    multiEQRestriction = (SingleColumnRestriction.SuperColumnMultiEQRestriction) restriction;
                else if (restriction instanceof SingleColumnRestriction.SuperColumnKeyEQRestriction)
                    keyEQRestriction = (SingleColumnRestriction.SuperColumnKeyEQRestriction) restriction;
            }

            this.multiSliceRestriction = multiSliceRestriction;
            this.keySliceRestriction = keySliceRestriction;
            this.keyINRestriction = keyINRestriction;
            this.multiEQRestriction = multiEQRestriction;
            this.keyEQRestriction = keyEQRestriction;
        }
    }
}
