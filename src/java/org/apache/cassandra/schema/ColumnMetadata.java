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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.SimpleSelector;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.github.jamm.Unmetered;

@Unmetered
public final class ColumnMetadata extends ColumnSpecification implements Selectable, Comparable<ColumnMetadata>
{
    public static final Comparator<Object> asymmetricColumnDataComparator =
        (a, b) -> ((ColumnData) a).column().compareTo((ColumnMetadata) b);

    public static final int NO_POSITION = -1;

    public enum ClusteringOrder
    {
        ASC, DESC, NONE
    }

    /**
     * The type of CQL3 column this definition represents.
     * There is 4 main type of CQL3 columns: those parts of the partition key,
     * those parts of the clustering columns and amongst the others, regular and
     * static ones.
     *
     * IMPORTANT: this enum is serialized as toString() and deserialized by calling
     * Kind.valueOf(), so do not override toString() or rename existing values.
     */
    public enum Kind
    {
        // NOTE: if adding a new type, must modify comparisonOrder
        PARTITION_KEY,
        CLUSTERING,
        REGULAR,
        STATIC;

        public boolean isPrimaryKeyKind()
        {
            return this == PARTITION_KEY || this == CLUSTERING;
        }

    }

    public final Kind kind;

    /*
     * If the column is a partition key or clustering column, its position relative to
     * other columns of the same kind. Otherwise,  NO_POSITION (-1).
     *
     * Note that partition key and clustering columns are numbered separately so
     * the first clustering column is 0.
     */
    private final int position;

    private final Comparator<CellPath> cellPathComparator;
    private final Comparator<Object> asymmetricCellPathComparator;
    private final Comparator<? super Cell<?>> cellComparator;

    private int hash;

    /**
     * These objects are compared frequently, so we encode several of their comparison components
     * into a single long value so that this can be done efficiently
     */
    private final long comparisonOrder;

    private static long comparisonOrder(Kind kind, boolean isComplex, long position, ColumnIdentifier name)
    {
        assert position >= 0 && position < 1 << 12;
        return   (((long) kind.ordinal()) << 61)
               | (isComplex ? 1L << 60 : 0)
               | (position << 48)
               | (name.prefixComparison >>> 16);
    }

    public static ColumnMetadata partitionKeyColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position)
    {
        return new ColumnMetadata(table, name, type, position, Kind.PARTITION_KEY);
    }

    public static ColumnMetadata partitionKeyColumn(String keyspace, String table, String name, AbstractType<?> type, int position)
    {
        return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, position, Kind.PARTITION_KEY);
    }

    public static ColumnMetadata clusteringColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position)
    {
        return new ColumnMetadata(table, name, type, position, Kind.CLUSTERING);
    }

    public static ColumnMetadata clusteringColumn(String keyspace, String table, String name, AbstractType<?> type, int position)
    {
        return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, position, Kind.CLUSTERING);
    }

    public static ColumnMetadata regularColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type)
    {
        return new ColumnMetadata(table, name, type, NO_POSITION, Kind.REGULAR);
    }

    public static ColumnMetadata regularColumn(String keyspace, String table, String name, AbstractType<?> type)
    {
        return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, NO_POSITION, Kind.REGULAR);
    }

    public static ColumnMetadata staticColumn(TableMetadata table, ByteBuffer name, AbstractType<?> type)
    {
        return new ColumnMetadata(table, name, type, NO_POSITION, Kind.STATIC);
    }

    public static ColumnMetadata staticColumn(String keyspace, String table, String name, AbstractType<?> type)
    {
        return new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, NO_POSITION, Kind.STATIC);
    }

    public ColumnMetadata(TableMetadata table, ByteBuffer name, AbstractType<?> type, int position, Kind kind)
    {
        this(table.keyspace,
             table.name,
             ColumnIdentifier.getInterned(name, UTF8Type.instance),
             type,
             position,
             kind);
    }

    @VisibleForTesting
    public ColumnMetadata(String ksName,
                          String cfName,
                          ColumnIdentifier name,
                          AbstractType<?> type,
                          int position,
                          Kind kind)
    {
        super(ksName, cfName, name, type);
        assert name != null && type != null && kind != null;
        assert (position == NO_POSITION) == !kind.isPrimaryKeyKind(); // The position really only make sense for partition and clustering columns (and those must have one),
                                                                      // so make sure we don't sneak it for something else since it'd breaks equals()
        this.kind = kind;
        this.position = position;
        this.cellPathComparator = makeCellPathComparator(kind, type);
        this.cellComparator = cellPathComparator == null ? ColumnData.comparator : (a, b) -> cellPathComparator.compare(a.path(), b.path());
        this.asymmetricCellPathComparator = cellPathComparator == null ? null : (a, b) -> cellPathComparator.compare(((Cell<?>)a).path(), (CellPath) b);
        this.comparisonOrder = comparisonOrder(kind, isComplex(), Math.max(0, position), name);
    }

    private static Comparator<CellPath> makeCellPathComparator(Kind kind, AbstractType<?> type)
    {
        if (kind.isPrimaryKeyKind() || !type.isMultiCell())
            return null;

        AbstractType<?> nameComparator = type.isCollection()
                                       ? ((CollectionType) type).nameComparator()
                                       : ((UserType) type).nameComparator();


        return (path1, path2) ->
        {
            if (path1.size() == 0 || path2.size() == 0)
            {
                if (path1 == CellPath.BOTTOM)
                    return path2 == CellPath.BOTTOM ? 0 : -1;
                if (path1 == CellPath.TOP)
                    return path2 == CellPath.TOP ? 0 : 1;
                return path2 == CellPath.BOTTOM ? 1 : -1;
            }

            // This will get more complicated once we have non-frozen UDT and nested collections
            assert path1.size() == 1 && path2.size() == 1;
            return nameComparator.compare(path1.get(0), path2.get(0));
        };
    }

    public ColumnMetadata copy()
    {
        return new ColumnMetadata(ksName, cfName, name, type, position, kind);
    }

    public ColumnMetadata withNewName(ColumnIdentifier newName)
    {
        return new ColumnMetadata(ksName, cfName, newName, type, position, kind);
    }

    public ColumnMetadata withNewType(AbstractType<?> newType)
    {
        return new ColumnMetadata(ksName, cfName, name, newType, position, kind);
    }

    public boolean isPartitionKey()
    {
        return kind == Kind.PARTITION_KEY;
    }

    public boolean isClusteringColumn()
    {
        return kind == Kind.CLUSTERING;
    }

    public boolean isStatic()
    {
        return kind == Kind.STATIC;
    }

    public boolean isRegular()
    {
        return kind == Kind.REGULAR;
    }

    public ClusteringOrder clusteringOrder()
    {
        if (!isClusteringColumn())
            return ClusteringOrder.NONE;

        return type.isReversed() ? ClusteringOrder.DESC : ClusteringOrder.ASC;
    }

    public int position()
    {
        return position;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ColumnMetadata))
            return false;

        ColumnMetadata cd = (ColumnMetadata) o;

        return equalsWithoutType(cd) && type.equals(cd.type);
    }

    private boolean equalsWithoutType(ColumnMetadata other)
    {
        return name.equals(other.name)
            && kind == other.kind
            && position == other.position
            && ksName.equals(other.ksName)
            && cfName.equals(other.cfName);
    }

    Optional<Difference> compare(ColumnMetadata other)
    {
        if (!equalsWithoutType(other))
            return Optional.of(Difference.SHALLOW);

        if (type.equals(other.type))
            return Optional.empty();

        return type.asCQL3Type().toString().equals(other.type.asCQL3Type().toString())
             ? Optional.of(Difference.DEEP)
             : Optional.of(Difference.SHALLOW);
    }

    @Override
    public int hashCode()
    {
        // This achieves the same as Objects.hashcode, but avoids the object array allocation
        // which features significantly in the allocation profile and caches the result.
        int result = hash;
        if (result == 0)
        {
            result = 31 + (ksName == null ? 0 : ksName.hashCode());
            result = 31 * result + (cfName == null ? 0 : cfName.hashCode());
            result = 31 * result + (name == null ? 0 : name.hashCode());
            result = 31 * result + (type == null ? 0 : type.hashCode());
            result = 31 * result + (kind == null ? 0 : kind.hashCode());
            result = 31 * result + position;
            hash = result;
        }
        return result;
    }

    @Override
    public String toString()
    {
        return name.toString();
    }

    public String debugString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("type", type)
                          .add("kind", kind)
                          .add("position", position)
                          .toString();
    }

    public boolean isPrimaryKeyColumn()
    {
        return kind.isPrimaryKeyKind();
    }

    @Override
    public boolean selectColumns(Predicate<ColumnMetadata> predicate)
    {
        return predicate.test(this);
    }

    @Override
    public boolean processesSelection()
    {
        return false;
    }

    /**
     * Converts the specified column definitions into column identifiers.
     *
     * @param definitions the column definitions to convert.
     * @return the column identifiers corresponding to the specified definitions
     */
    public static Collection<ColumnIdentifier> toIdentifiers(Collection<ColumnMetadata> definitions)
    {
        return Collections2.transform(definitions, columnDef -> columnDef.name);
    }

    public int compareTo(ColumnMetadata other)
    {
        if (this == other)
            return 0;

        if (comparisonOrder != other.comparisonOrder)
            return Long.compare(comparisonOrder, other.comparisonOrder);

        return this.name.compareTo(other.name);
    }

    public Comparator<CellPath> cellPathComparator()
    {
        return cellPathComparator;
    }

    public Comparator<Object> asymmetricCellPathComparator()
    {
        return asymmetricCellPathComparator;
    }

    public Comparator<? super Cell<?>> cellComparator()
    {
        return cellComparator;
    }

    public boolean isComplex()
    {
        return cellPathComparator != null;
    }

    public boolean isSimple()
    {
        return !isComplex();
    }

    public CellPath.Serializer cellPathSerializer()
    {
        // Collections are our only complex so far, so keep it simple
        return CollectionType.cellPathSerializer;
    }

    public <V> void validateCell(Cell<V> cell)
    {
        if (cell.isTombstone())
        {
            if (cell.valueSize() > 0)
                throw new MarshalException("A tombstone should not have a value");
            if (cell.path() != null)
                validateCellPath(cell.path());
        }
        else if(type.isUDT())
        {
            // To validate a non-frozen UDT field, both the path and the value
            // are needed, the path being an index into an array of value types.
            ((UserType)type).validateCell(cell);
        }
        else
        {
            type.validateCellValue(cell.value(), cell.accessor());
            if (cell.path() != null)
                validateCellPath(cell.path());
        }
    }

    private void validateCellPath(CellPath path)
    {
        if (!isComplex())
            throw new MarshalException("Only complex cells should have a cell path");

        assert type.isMultiCell();
        if (type.isCollection())
            ((CollectionType)type).nameComparator().validate(path.get(0));
        else
            ((UserType)type).nameComparator().validate(path.get(0));
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(name)
               .append(' ')
               .append(type);

        if (isStatic())
            builder.append(" static");
    }

    public static String toCQLString(Iterable<ColumnMetadata> defs)
    {
        return toCQLString(defs.iterator());
    }

    public static String toCQLString(Iterator<ColumnMetadata> defs)
    {
        if (!defs.hasNext())
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append(defs.next().name.toCQLString());
        while (defs.hasNext())
            sb.append(", ").append(defs.next().name.toCQLString());
        return sb.toString();
    }


    public void appendNameAndOrderTo(CqlBuilder builder)
    {
        builder.append(name.toCQLString())
               .append(' ')
               .append(clusteringOrder().toString());
    }

    /**
     * The type of the cell values for cell belonging to this column.
     *
     * This is the same than the column type, except for non-frozen collections where it's the 'valueComparator'
     * of the collection.
     * 
     * This method should not be used to get value type of non-frozon UDT.
     */
    public AbstractType<?> cellValueType()
    {
        assert !(type instanceof UserType && type.isMultiCell());
        return type instanceof CollectionType && type.isMultiCell()
                ? ((CollectionType)type).valueComparator()
                : type;
    }

    /**
     * Check if column is counter type.
     */
    public boolean isCounterColumn()
    {
        if (type instanceof CollectionType) // Possible with, for example, supercolumns
            return ((CollectionType) type).valueComparator().isCounter();
        return type.isCounter();
    }

    public Selector.Factory newSelectorFactory(TableMetadata table, AbstractType<?> expectedType, List<ColumnMetadata> defs, VariableSpecifications boundNames) throws InvalidRequestException
    {
        return SimpleSelector.newFactory(this, addAndGetIndex(this, defs));
    }

    public AbstractType<?> getExactTypeIfKnown(String keyspace)
    {
        return type;
    }
}
