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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.ast.Symbol.UnquotedSymbol;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;

import static org.apache.cassandra.cql3.ast.Elements.newLine;

//TODO (now): replace with IndexMetadata?  Rather than create a custom DDL type can just leverage the existing metadata like Table/Keyspace
public class CreateIndexDDL implements Element
{
    public enum Version { V1, V2 }

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
        if (AbstractTypeGenerators.contains(type, DurationType.instance)) return false; //TODO (correctness): remove once fixed in CASSANDRA-19890; type.referencesDuration is not correct in all cases
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
    public void toCQL(StringBuilder sb, int indent)
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
            name.get().toCQL(sb, indent);
        }
        newLine(sb, indent);
        sb.append("ON ");
        on.toCQL(sb, indent);
        sb.append('(');
        for (ReferenceExpression ref : references)
        {
            ref.toCQL(sb, indent);
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
            newLine(sb, indent);
            sb.append("USING '");
            indexerName.toCQL(sb, indent);
            sb.append("'");
        }
        if (!options.isEmpty())
        {
            newLine(sb, indent);
            sb.append("WITH OPTIONS = {");
            for (Map.Entry<String, String> e : options.entrySet())
                sb.append("'").append(e.getKey()).append("': '").append(e.getValue()).append("', ");
            sb.setLength(sb.length() - 2); // remove ", "
            sb.append('}');
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
        public void toCQL(StringBuilder sb, int indent)
        {
            sb.append(kind.name()).append('(');
            column.toCQL(sb, indent);
            sb.append(')');
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(column);
        }
    }
}
