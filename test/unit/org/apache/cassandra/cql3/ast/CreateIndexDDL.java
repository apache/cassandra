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

package org.apache.cassandra.cql3.ast;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.ast.Symbol.UnquotedSymbol;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

//TODO (now): replace with IndexMetadata?  Rather than create a custom DDL type can just leverage the existing metadata like Table/Keyspace
public class CreateIndexDDL implements Element
{
    public enum Version { V1, V2 }
    public enum QueryType { Eq, Range }

    public interface Indexer
    {
        enum Kind { legacy, sai }
        Kind kind();
        default boolean isCustom()
        {
            return kind() != Kind.legacy;
        }
        UnquotedSymbol name();
        default UnquotedSymbol longName()
        {
            return name();
        }
        boolean supported(TableMetadata table, ColumnMetadata column);
        default EnumSet<QueryType> supportedQueries(AbstractType<?> type)
        {
            return EnumSet.of(QueryType.Eq);
        }
    }

    public static List<Indexer> supportedIndexers()
    {
        return Arrays.asList(LEGACY, SAI);
    }

    private static boolean standardSupported(TableMetadata metadata, ColumnMetadata col)
    {
        if (metadata.partitionKeyColumns().size() == 1 && col.isPartitionKey()) return false;
        AbstractType<?> type = col.type.unwrap();
        if (type.isUDT() && type.isMultiCell()) return false; // non-frozen UDTs are not supported
        if (type.referencesDuration()) return false; // Duration is not allowed!  See org.apache.cassandra.cql3.statements.schema.CreateIndexStatement.validateIndexTarget
        return true;
    }

    private static boolean isFrozen(AbstractType<?> type)
    {
        return !type.subTypes().isEmpty() && !type.isMultiCell();
    }

    public static final Indexer LEGACY = new Indexer()
    {

        @Override
        public Kind kind()
        {
            return Kind.legacy;
        }

        @Override
        public UnquotedSymbol name()
        {
            return new UnquotedSymbol("legacy_local_table", UTF8Type.instance);
        }

        @Override
        public boolean supported(TableMetadata table, ColumnMetadata column)
        {
            return standardSupported(table, column);
        }
    };

    private static final Set<AbstractType<?>> SAI_EQ_ONLY = ImmutableSet.of(UTF8Type.instance, AsciiType.instance,
                                                                            BooleanType.instance,
                                                                            UUIDType.instance);

    public static final Indexer SAI = new Indexer()
    {
        @Override
        public Kind kind()
        {
            return Kind.sai;
        }

        @Override
        public UnquotedSymbol name()
        {
            return new UnquotedSymbol("SAI", UTF8Type.instance);
        }

        @Override
        public UnquotedSymbol longName()
        {
            return new UnquotedSymbol("StorageAttachedIndex", UTF8Type.instance);
        }

        @Override
        public boolean supported(TableMetadata table, ColumnMetadata col)
        {
            if (!standardSupported(table, col)) return false;
            AbstractType<?> type = col.type.unwrap();
            if (type instanceof CompositeType)
            {
                // each element must be SUPPORTED_TYPES only...
                if (type.subTypes().stream().allMatch(StorageAttachedIndex.SUPPORTED_TYPES::contains))
                    return true;
            }
            else if (((isFrozen(type) && !type.isVector()) || StorageAttachedIndex.SUPPORTED_TYPES.contains(type.asCQL3Type())))
                return true;
            return false;
        }

        @Override
        public EnumSet<QueryType> supportedQueries(AbstractType<?> type)
        {
            type = type.unwrap();
            if (IndexTermType.isEqOnlyType(type) || type.isCollection() || type.isUDT() || type.isTuple())
                return EnumSet.of(QueryType.Eq);
            return EnumSet.allOf(QueryType.class);
        }
    };

    public final Version version;
    public final Indexer indexer;
    public final Optional<Symbol> name;
    public final TableReference on;
    public final List<ReferenceExpression> references;
    public final Map<String, String> options;

    public CreateIndexDDL(Version version, Indexer indexer, Optional<Symbol> name, TableReference on, List<ReferenceExpression> references, Map<String, String> options)
    {
        this.version = version;
        this.indexer = indexer;
        this.name = name;
        this.on = on;
        this.references = references;
        this.options = options;
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        switch (version)
        {
            case V1:
                if (indexer.isCustom()) sb.append("CREATE CUSTOM INDEX");
                else                    sb.append("CREATE INDEX");
                break;
            case V2:
                sb.append("CREATE INDEX");
                break;
            default:
                throw new UnsupportedOperationException(version.name());
        }
        if (name.isPresent())
        {
            sb.append(' ');
            name.get().toCQL(sb, formatter);
        }
        formatter.section(sb);
        sb.append("ON ");
        on.toCQL(sb, formatter);
        sb.append('(');
        for (ReferenceExpression ref : references)
        {
            ref.toCQL(sb, formatter);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2); // remove last ", "
        sb.append(')');
        UnquotedSymbol indexerName = null;
        switch (version)
        {
            case V1:
                if (indexer.isCustom())
                    indexerName = indexer.longName();
                break;
            case V2:
                indexerName = indexer.name();
                break;
            default:
                throw new UnsupportedOperationException(version.name());
        }
        if (indexerName != null)
        {
            formatter.section(sb);
            sb.append("USING '");
            indexerName.toCQL(sb, formatter);
            sb.append("'");
        }
        if (!options.isEmpty())
        {
            formatter.section(sb);
            sb.append("WITH OPTIONS = {");
            for (Map.Entry<String, String> e : options.entrySet())
                sb.append("'").append(e.getKey()).append("': '").append(e.getValue()).append("', ");
            sb.setLength(sb.length() - 2); // remove ", "
            sb.append('}');
        }
    }

    public static class IndexedColumn
    {
        public final Symbol symbol;
        public final CreateIndexDDL indexDDL;

        public IndexedColumn(Symbol symbol, CreateIndexDDL indexDDL)
        {
            this.symbol = symbol;
            this.indexDDL = indexDDL;
        }

        public EnumSet<CreateIndexDDL.QueryType> supportedQueries()
        {
            return indexDDL.indexer.supportedQueries(symbol.type());
        }
    }

    public static class CollectionReference implements ReferenceExpression
    {
        public enum Kind { FULL, KEYS, ENTRIES }

        public final Kind kind;
        public final ReferenceExpression column;

        public CollectionReference(Kind kind, ReferenceExpression column)
        {
            this.kind = kind;
            this.column = column;
        }

        @Override
        public AbstractType<?> type()
        {
            return column.type();
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append(kind.name()).append('(');
            column.toCQL(sb, formatter);
            sb.append(')');
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(column);
        }
    }
}
