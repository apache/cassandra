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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.InvalidRequestException;
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
    public final Name key;
    // LinkedHashMap because the order does matter (it is the order in the composite type)
    public final LinkedHashMap<ColumnIdentifier, Name> columns = new LinkedHashMap<ColumnIdentifier, Name>();
    public final Name value;
    // Keep metadata lexicographically ordered so that wildcard expansion have a deterministic order
    public final Map<ColumnIdentifier, Name> metadata = new TreeMap<ColumnIdentifier, Name>();

    public final boolean isComposite;
    // Note that isCompact means here that no componet of the comparator correspond to the column names
    // defined in the CREATE TABLE QUERY. This is not exactly equivalent to the 'WITH COMPACT STORAGE'
    // option when creating a table in that "static CF" without a composite type will have isCompact == false
    // even though one must use 'WITH COMPACT STORAGE' to declare them.
    public final boolean isCompact;

    public CFDefinition(CFMetaData cfm)
    {
        this.cfm = cfm;
        this.key = new Name(cfm.ksName, cfm.cfName, getKeyId(cfm), Name.Kind.KEY_ALIAS, cfm.getKeyValidator());
        if (cfm.comparator instanceof CompositeType)
        {
            this.isComposite = true;
            CompositeType composite = (CompositeType)cfm.comparator;
            if (cfm.getColumn_metadata().isEmpty())
            {
                // "dense" composite
                this.isCompact = true;
                for (int i = 0; i < composite.types.size(); i++)
                {
                    ColumnIdentifier id = getColumnId(cfm, i);
                    this.columns.put(id, new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, i, composite.types.get(i)));
                }
                this.value = new Name(cfm.ksName, cfm.cfName, getValueId(cfm), Name.Kind.VALUE_ALIAS, cfm.getDefaultValidator());
            }
            else
            {
                // "sparse" composite
                this.isCompact = false;
                this.value = null;
                assert cfm.getValueAlias() == null;
                for (int i = 0; i < composite.types.size() - 1; i++)
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
        }
        else
        {
            this.isComposite = false;
            if (cfm.getColumn_metadata().isEmpty())
            {
                // dynamic CF
                this.isCompact = true;
                ColumnIdentifier id = getColumnId(cfm, 0);
                Name name = new Name(cfm.ksName, cfm.cfName, id, Name.Kind.COLUMN_ALIAS, 0, cfm.comparator);
                this.columns.put(id, name);
                this.value = new Name(cfm.ksName, cfm.cfName, getValueId(cfm), Name.Kind.VALUE_ALIAS, cfm.getDefaultValidator());
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

    private static ColumnIdentifier getKeyId(CFMetaData cfm)
    {
        return cfm.getKeyAlias() == null
             ? new ColumnIdentifier(DEFAULT_KEY_ALIAS, false)
             : new ColumnIdentifier(cfm.getKeyAlias(), definitionType);
    }

    private static ColumnIdentifier getColumnId(CFMetaData cfm, int i)
    {
        List<ByteBuffer> definedNames = cfm.getColumnAliases();
        return definedNames == null || i >= definedNames.size()
             ? new ColumnIdentifier(DEFAULT_COLUMN_ALIAS + (i + 1), false)
             : new ColumnIdentifier(cfm.getColumnAliases().get(i), definitionType);
    }

    private static ColumnIdentifier getValueId(CFMetaData cfm)
    {
        return cfm.getValueAlias() == null
             ? new ColumnIdentifier(DEFAULT_VALUE_ALIAS, false)
             : new ColumnIdentifier(cfm.getValueAlias(), definitionType);
    }

    public Name get(ColumnIdentifier name)
    {
        if (name.equals(key.name))
            return key;
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
            private boolean keyDone;
            private final Iterator<Name> columnIter = columns.values().iterator();
            private boolean valueDone;
            private final Iterator<Name> metadataIter = metadata.values().iterator();

            protected Name computeNext()
            {
                if (!keyDone)
                {
                    keyDone = true;
                    return key;
                }

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
        public final int position; // only make sense for COLUMN_ALIAS
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(key.name);
        for (Name name : columns.values())
            sb.append(", ").append(name.name);
        sb.append(" => ");
        if (value != null)
            sb.append(value.name);
        if (!metadata.isEmpty())
        {
            sb.append("{");
            for (Name name : metadata.values())
                sb.append(" ").append(name.name);
            sb.append(" }");
        }
        sb.append("]");
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

        public NonCompositeBuilder add(Term t, Relation.Type op, List<ByteBuffer> variables) throws InvalidRequestException
        {
            if (columnName != null)
                throw new IllegalStateException("Column name is already constructed");

            // We don't support the relation type yet, i.e., there is no distinction between x > 3 and x >= 3.
            columnName = t.getByteBuffer(type, variables);
            return this;
        }

        public int componentCount()
        {
            return columnName == null ? 0 : 1;
        }

        public ByteBuffer build()
        {
            return columnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnName;
        }

        public ByteBuffer buildAsEndOfRange()
        {
            throw new IllegalStateException();
        }

        public NonCompositeBuilder copy()
        {
            NonCompositeBuilder newBuilder = new NonCompositeBuilder(type);
            newBuilder.columnName = columnName;
            return newBuilder;
        }
    }
}
