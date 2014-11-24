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
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Holds metadata on a CF preprocessed for use by CQL queries.
 */
public class CFDefinition implements Iterable<CFDefinition.Name>
{
    public static final AbstractType<?> definitionType = UTF8Type.instance;

    // Keep static and regular columns lexicographically ordered so that wildcard expansion have a deterministic order
    private static final Comparator<ColumnIdentifier> identifierComparator = new Comparator<ColumnIdentifier>()
    {
        public int compare(ColumnIdentifier id1, ColumnIdentifier id2)
        {
            return ByteBufferUtil.compareUnsigned(id1.key, id2.key);
        }
    };

    public final CFMetaData cfm;
    // LinkedHashMap because the order does matter (it is the order in the composite type)
    private final LinkedHashMap<ColumnIdentifier, Name> partitionKeys = new LinkedHashMap<ColumnIdentifier, Name>();
    private final LinkedHashMap<ColumnIdentifier, Name> clusteringColumns = new LinkedHashMap<ColumnIdentifier, Name>();
    private final Name compactValue;
    // Keep metadata lexicographically ordered so that wildcard expansion have a deterministic order
    private final Map<ColumnIdentifier, Name> staticColumns = new TreeMap<ColumnIdentifier, Name>(identifierComparator);
    private final Map<ColumnIdentifier, Name> regularColumns = new TreeMap<ColumnIdentifier, Name>(identifierComparator);

    public final boolean isComposite;
    public final boolean hasCompositeKey;
    // Note that isCompact means here that no componet of the comparator correspond to the column names
    // defined in the CREATE TABLE QUERY. This is not exactly equivalent to the 'WITH COMPACT STORAGE'
    // option when creating a table in that "static CF" without a composite type will have isCompact == false
    // even though one must use 'WITH COMPACT STORAGE' to declare them.
    public final boolean isCompact;
    public final boolean hasCollections;

    public CFDefinition(CFMetaData cfm)
    {
        this.cfm = cfm;

        this.hasCompositeKey = cfm.getKeyValidator() instanceof CompositeType;
        for (int i = 0; i < cfm.partitionKeyColumns().size(); ++i)
        {
            ColumnIdentifier id = new ColumnIdentifier(cfm.partitionKeyColumns().get(i).name, definitionType);
            this.partitionKeys.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.KEY_ALIAS, i, cfm.getKeyValidator().getComponents().get(i)));
        }

        this.isComposite = cfm.comparator instanceof CompositeType;
        this.hasCollections = cfm.comparator.getComponents().get(cfm.comparator.componentsCount() - 1) instanceof ColumnToCollectionType;
        this.isCompact = cfm.clusteringKeyColumns().size() == cfm.comparator.componentsCount();
        for (int i = 0; i < cfm.clusteringKeyColumns().size(); ++i)
        {
            ColumnIdentifier id = new ColumnIdentifier(cfm.clusteringKeyColumns().get(i).name, definitionType);
            this.clusteringColumns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, i, cfm.comparator.getComponents().get(i)));
        }

        if (isCompact)
        {
            this.compactValue = createValue(cfm);
        }
        else
        {
            this.compactValue = null;
            for (ColumnDefinition def : cfm.regularColumns())
            {
                ColumnIdentifier id = new ColumnIdentifier(def.name, cfm.getColumnDefinitionComparator(def));
                this.regularColumns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_METADATA, def.getValidator()));
            }
            for (ColumnDefinition def : cfm.staticColumns())
            {
                ColumnIdentifier id = new ColumnIdentifier(def.name, cfm.getColumnDefinitionComparator(def));
                this.staticColumns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.STATIC, def.getValidator()));
            }
        }
    }

    public ColumnToCollectionType getCollectionType()
    {
        if (!hasCollections)
            return null;

        CompositeType composite = (CompositeType)cfm.comparator;
        return (ColumnToCollectionType)composite.types.get(composite.types.size() - 1);
    }

    private static Name createValue(CFMetaData cfm)
    {
        ColumnIdentifier alias = new ColumnIdentifier(cfm.compactValueColumn().name, definitionType);
        // That's how we distinguish between 'no value alias because coming from thrift' and 'I explicitely did not
        // define a value' (see CreateTableStatement)
        return alias.key.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
               ? null
               : new Name(cfm.ksName, cfm.cfName, alias, Name.Kind.VALUE_ALIAS, cfm.getDefaultValidator());
    }

    public int partitionKeyCount()
    {
        return partitionKeys.size();
    }

    public Collection<Name> partitionKeys()
    {
        return partitionKeys.values();
    }

    public int clusteringColumnsCount()
    {
        return clusteringColumns.size();
    }

    public Collection<Name> clusteringColumns()
    {
        return clusteringColumns.values();
    }

    public Collection<Name> regularColumns()
    {
        return regularColumns.values();
    }

    public Collection<Name> staticColumns()
    {
        return staticColumns.values();
    }

    public Name compactValue()
    {
        return compactValue;
    }

    public Name get(ColumnIdentifier name)
    {
        CFDefinition.Name def = partitionKeys.get(name);
        if (def != null)
            return def;
        if (compactValue != null && name.equals(compactValue.name))
            return compactValue;
        def = clusteringColumns.get(name);
        if (def != null)
            return def;
        def = regularColumns.get(name);
        if (def != null)
            return def;
        return staticColumns.get(name);
    }

    public Iterator<Name> iterator()
    {
        return new AbstractIterator<Name>()
        {
            private final Iterator<Name> keyIter = partitionKeys.values().iterator();
            private final Iterator<Name> clusteringIter = clusteringColumns.values().iterator();
            private boolean valueDone;
            private final Iterator<Name> staticIter = staticColumns.values().iterator();
            private final Iterator<Name> regularIter = regularColumns.values().iterator();

            protected Name computeNext()
            {
                if (keyIter.hasNext())
                    return keyIter.next();

                if (clusteringIter.hasNext())
                    return clusteringIter.next();

                if (compactValue != null && !valueDone)
                {
                    valueDone = true;
                    return compactValue;
                }

                if (staticIter.hasNext())
                    return staticIter.next();

                if (regularIter.hasNext())
                    return regularIter.next();

                return endOfData();
            }
        };
    }

    public ColumnNameBuilder getKeyNameBuilder()
    {
        return hasCompositeKey
             ? new CompositeType.Builder((CompositeType)cfm.getKeyValidator())
             : new NonCompositeBuilder(cfm.getKeyValidator());
    }

    public ColumnNameBuilder getColumnNameBuilder()
    {
        return isComposite
             ? new CompositeType.Builder((CompositeType)cfm.comparator)
             : new NonCompositeBuilder(cfm.comparator);
    }

    public static class Name extends ColumnSpecification
    {
        public static enum Kind
        {
            KEY_ALIAS, COLUMN_ALIAS, VALUE_ALIAS, COLUMN_METADATA, STATIC
        }

        private Name(String ksName, String cfName, ColumnIdentifier name, Kind kind, AbstractType<?> type)
        {
            this(ksName, cfName, name, kind, -1, type);
        }

        private Name(String ksName, String cfName, ColumnIdentifier name, Kind kind, int position, AbstractType<?> type)
        {
            super(ksName, cfName, name, type);
            this.kind = kind;
            this.position = position;
        }

        public final Kind kind;
        public final int position; // only make sense for KEY_ALIAS and COLUMN_ALIAS

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof Name))
                return false;
            Name that = (Name)o;
            return Objects.equal(ksName, that.ksName)
                && Objects.equal(cfName, that.cfName)
                && Objects.equal(name, that.name)
                && Objects.equal(type, that.type)
                && kind == that.kind
                && position == that.position;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(ksName, cfName, name, type, kind, position);
        }

        public boolean isPrimaryKeyColumn()
        {
            return kind == Kind.KEY_ALIAS || kind == Kind.COLUMN_ALIAS;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(Joiner.on(", ").join(partitionKeys.values()));
        if (!clusteringColumns.isEmpty())
            sb.append(", ").append(Joiner.on(", ").join(clusteringColumns.values()));
        sb.append(" => ");
        if (compactValue != null)
            sb.append(compactValue.name);
        sb.append("{");
        sb.append(Joiner.on(", ").join(staticColumns.values()));
        if (!staticColumns.isEmpty())
            sb.append(", ");
        sb.append(Joiner.on(", ").join(regularColumns.values()));
        sb.append("}");
        return sb.toString();
    }

    private static class NonCompositeBuilder implements ColumnNameBuilder
    {
        private final AbstractType<?> type;
        private ByteBuffer columnName;

        private NonCompositeBuilder(AbstractType<?> type)
        {
            this.type = type;
        }

        public NonCompositeBuilder add(ByteBuffer bb)
        {
            if (columnName != null)
                throw new IllegalStateException("Column name is already constructed");

            columnName = bb;
            return this;
        }

        public int componentCount()
        {
            return columnName == null ? 0 : 1;
        }

        public int remainingCount()
        {
            return columnName == null ? 1 : 0;
        }

        public ByteBuffer get(int i)
        {
            if (i < 0 || i >= (columnName == null ? 0 : 1))
                throw new IllegalArgumentException();

            return columnName;
        }

        public ByteBuffer build()
        {
            return columnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnName;
        }

        public ByteBuffer buildAsEndOfRange()
        {
            return build();
        }

        public ByteBuffer buildForRelation(Relation.Type op)
        {
            return build();
        }

        public NonCompositeBuilder copy()
        {
            NonCompositeBuilder newBuilder = new NonCompositeBuilder(type);
            newBuilder.columnName = columnName;
            return newBuilder;
        }

        public ByteBuffer getComponent(int i)
        {
            if (i != 0 || columnName == null)
                throw new IllegalArgumentException();

            return columnName;
        }

        public int getLength()
        {
            return columnName == null ? 0 : columnName.remaining();
        }
    }
}
