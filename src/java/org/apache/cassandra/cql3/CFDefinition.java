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

    private static final String DEFAULT_KEY_ALIAS = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS = "value";

    public final CFMetaData cfm;
    // LinkedHashMap because the order does matter (it is the order in the composite type)
    public final LinkedHashMap<ColumnIdentifier, Name> keys = new LinkedHashMap<ColumnIdentifier, Name>();
    public final LinkedHashMap<ColumnIdentifier, Name> columns = new LinkedHashMap<ColumnIdentifier, Name>();
    public final Name value;
    // Keep metadata lexicographically ordered so that wildcard expansion have a deterministic order
    public final Map<ColumnIdentifier, Name> metadata = new TreeMap<ColumnIdentifier, Name>();

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

        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            this.hasCompositeKey = true;
            CompositeType keyComposite = (CompositeType)cfm.getKeyValidator();
            assert keyComposite.types.size() > 1;
            for (int i = 0; i < keyComposite.types.size(); i++)
            {
                ColumnIdentifier id = getKeyId(cfm, i);
                this.keys.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.KEY_ALIAS, i, keyComposite.types.get(i)));
            }
        }
        else
        {
            this.hasCompositeKey = false;
            ColumnIdentifier id = getKeyId(cfm, 0);
            this.keys.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.KEY_ALIAS, 0, cfm.getKeyValidator()));
        }

        if (cfm.comparator instanceof CompositeType)
        {
            this.isComposite = true;
            CompositeType composite = (CompositeType)cfm.comparator;
            /*
             * We are a "sparse" composite, i.e. a non-compact one, if either:
             *   - the last type of the composite is a ColumnToCollectionType
             *   - or we have one less alias than of composite types and the last type is UTF8Type.
             *
             * Note that this is not perfect: if someone upgrading from thrift "renames" all but
             * the last column alias, the cf will be considered "sparse" and he will be stuck with
             * that even though that might not be what he wants. But the simple workaround is
             * for that user to rename all the aliases at the same time in the first place.
             */
            int last = composite.types.size() - 1;
            AbstractType<?> lastType = composite.types.get(last);
            if (lastType instanceof ColumnToCollectionType
                || (cfm.getColumnAliases().size() == last && lastType instanceof UTF8Type))
            {
                // "sparse" composite
                this.isCompact = false;
                this.value = null;
                assert cfm.getValueAlias() == null;
                // check for collection type
                if (lastType instanceof ColumnToCollectionType)
                {
                    --last;
                    this.hasCollections = true;
                }
                else
                {
                    this.hasCollections = false;
                }

                for (int i = 0; i < last; i++)
                {
                    ColumnIdentifier id = getColumnId(cfm, i);
                    this.columns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, i, composite.types.get(i)));
                }

                for (Map.Entry<ByteBuffer, ColumnDefinition> def : cfm.getColumn_metadata().entrySet())
                {
                    ColumnIdentifier id = new ColumnIdentifier(def.getKey(), cfm.getColumnDefinitionComparator(def.getValue()));
                    this.metadata.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_METADATA, def.getValue().getValidator()));
                }
            }
            else
            {
                // "dense" composite
                this.isCompact = true;
                this.hasCollections = false;
                for (int i = 0; i < composite.types.size(); i++)
                {
                    ColumnIdentifier id = getColumnId(cfm, i);
                    this.columns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, i, composite.types.get(i)));
                }
                this.value = createValue(cfm);
            }
        }
        else
        {
            this.isComposite = false;
            this.hasCollections = false;
            if (!cfm.getColumnAliases().isEmpty() || cfm.getColumn_metadata().isEmpty())
            {
                // dynamic CF
                this.isCompact = true;
                ColumnIdentifier id = getColumnId(cfm, 0);
                Name name = new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, 0, cfm.comparator);
                this.columns.put(id, name);
                this.value = createValue(cfm);
            }
            else
            {
                // static CF
                this.isCompact = false;
                this.value = null;
                assert cfm.getValueAlias() == null;
                assert cfm.getColumnAliases() == null || cfm.getColumnAliases().isEmpty();
                for (Map.Entry<ByteBuffer, ColumnDefinition> def : cfm.getColumn_metadata().entrySet())
                {
                    ColumnIdentifier id = new ColumnIdentifier(def.getKey(), cfm.getColumnDefinitionComparator(def.getValue()));
                    this.metadata.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_METADATA, def.getValue().getValidator()));
                }
            }
        }
        assert value == null || metadata.isEmpty();
    }

    private static ColumnIdentifier getKeyId(CFMetaData cfm, int i)
    {
        List<ByteBuffer> definedNames = cfm.getKeyAliases();
        // For compatibility sake, non-composite key default alias is 'key', not 'key1'.
        return definedNames == null || i >= definedNames.size() || cfm.getKeyAliases().get(i) == null
             ? new ColumnIdentifier(i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1), false)
             : new ColumnIdentifier(cfm.getKeyAliases().get(i), definitionType);
    }

    private static ColumnIdentifier getColumnId(CFMetaData cfm, int i)
    {
        List<ByteBuffer> definedNames = cfm.getColumnAliases();
        return definedNames == null || i >= definedNames.size() || cfm.getColumnAliases().get(i) == null
             ? new ColumnIdentifier(DEFAULT_COLUMN_ALIAS + (i + 1), false)
             : new ColumnIdentifier(cfm.getColumnAliases().get(i), definitionType);
    }

    private static ColumnIdentifier getValueId(CFMetaData cfm)
    {
        return cfm.getValueAlias() == null
             ? new ColumnIdentifier(DEFAULT_VALUE_ALIAS, false)
             : new ColumnIdentifier(cfm.getValueAlias(), definitionType);
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
        ColumnIdentifier alias = getValueId(cfm);
        // That's how we distinguish between 'no value alias because coming from thrift' and 'I explicitely did not
        // define a value' (see CreateColumnFamilyStatement)
        return alias.key.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
               ? null
               : new Name(cfm.ksName, cfm.cfName, alias, Name.Kind.VALUE_ALIAS, cfm.getDefaultValidator());
    }

    public Name get(ColumnIdentifier name)
    {
        CFDefinition.Name kdef = keys.get(name);
        if (kdef != null)
            return kdef;
        if (value != null && name.equals(value.name))
            return value;
        CFDefinition.Name def = columns.get(name);
        if (def != null)
            return def;
        return metadata.get(name);
    }

    public Iterator<Name> iterator()
    {
        return new AbstractIterator<Name>()
        {
            private final Iterator<Name> keyIter = keys.values().iterator();
            private final Iterator<Name> columnIter = columns.values().iterator();
            private boolean valueDone;
            private final Iterator<Name> metadataIter = metadata.values().iterator();

            protected Name computeNext()
            {
                if (keyIter.hasNext())
                    return keyIter.next();

                if (columnIter.hasNext())
                    return columnIter.next();

                if (value != null && !valueDone)
                {
                    valueDone = true;
                    return value;
                }

                if (metadataIter.hasNext())
                    return metadataIter.next();

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
            KEY_ALIAS, COLUMN_ALIAS, VALUE_ALIAS, COLUMN_METADATA
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
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(Joiner.on(", ").join(keys.values()));
        if (!columns.isEmpty())
            sb.append(", ").append(Joiner.on(", ").join(columns.values()));
        sb.append(" => ");
        if (value != null)
            sb.append(value.name);
        if (!metadata.isEmpty())
            sb.append("{").append(Joiner.on(", ").join(metadata.values())).append(" }");
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

        public NonCompositeBuilder add(ByteBuffer bb, Relation.Type op)
        {
            return add(bb);
        }

        public int componentCount()
        {
            return columnName == null ? 0 : 1;
        }

        public int remainingCount()
        {
            return columnName == null ? 1 : 0;
        }

        public ByteBuffer build()
        {
            return columnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnName;
        }

        public ByteBuffer buildAsEndOfRange()
        {
            return build();
        }

        public NonCompositeBuilder copy()
        {
            NonCompositeBuilder newBuilder = new NonCompositeBuilder(type);
            newBuilder.columnName = columnName;
            return newBuilder;
        }
    }
}
