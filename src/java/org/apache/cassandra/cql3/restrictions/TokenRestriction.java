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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * <code>Restriction</code> using the token function.
 */
public abstract class TokenRestriction extends AbstractPrimaryKeyRestrictions
{
    /**
     * The definition of the columns to which apply the token restriction.
     */
    protected final List<ColumnDefinition> columnDefs;

    final CFMetaData metadata;

    /**
     * Creates a new <code>TokenRestriction</code> that apply to the specified columns.
     *
     * @param columnDefs the definition of the columns to which apply the token restriction
     */
    public TokenRestriction(CFMetaData metadata, List<ColumnDefinition> columnDefs)
    {
        super(metadata.getKeyValidatorAsClusteringComparator());
        this.columnDefs = columnDefs;
        this.metadata = metadata;
    }

    @Override
    public  boolean isOnToken()
    {
        return true;
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
    {
        return columnDefs;
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return columnDefs.get(0);
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return columnDefs.get(columnDefs.size() - 1);
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager secondaryIndexManager)
    {
        return false;
    }

    @Override
    public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options)
    {
        throw new UnsupportedOperationException("Index expression cannot be created for token restriction");
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the column names as a comma separated <code>String</code>.
     *
     * @return the column names as a comma separated <code>String</code>.
     */
    protected final String getColumnNamesAsString()
    {
        return Joiner.on(", ").join(ColumnDefinition.toIdentifiers(columnDefs));
    }

    @Override
    public final PrimaryKeyRestrictions mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
        if (!otherRestriction.isOnToken())
            return new TokenFilter(toPrimaryKeyRestriction(otherRestriction), this);

        return doMergeWith((TokenRestriction) otherRestriction);
    }

    /**
     * Merges this restriction with the specified <code>TokenRestriction</code>.
     * @param otherRestriction the <code>TokenRestriction</code> to merge with.
     */
    protected abstract PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException;

    /**
     * Converts the specified restriction into a <code>PrimaryKeyRestrictions</code>.
     *
     * @param restriction the restriction to convert
     * @return a <code>PrimaryKeyRestrictions</code>
     * @throws InvalidRequestException if a problem occurs while converting the restriction
     */
    private PrimaryKeyRestrictions toPrimaryKeyRestriction(Restriction restriction) throws InvalidRequestException
    {
        if (restriction instanceof PrimaryKeyRestrictions)
            return (PrimaryKeyRestrictions) restriction;

        return new PrimaryKeyRestrictionSet(comparator, true).mergeWith(restriction);
    }

    public static final class EQRestriction extends TokenRestriction
    {
        private final Term value;

        public EQRestriction(CFMetaData cfm, List<ColumnDefinition> columnDefs, Term value)
        {
            super(cfm, columnDefs);
            this.value = value;
        }

        @Override
        public boolean isEQ()
        {
            return true;
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return value.getFunctions();
        }

        @Override
        protected PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 Joiner.on(", ").join(ColumnDefinition.toIdentifiers(columnDefs)));
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(value.bindAndGet(options));
        }
    }

    public static class SliceRestriction extends TokenRestriction
    {
        private final TermSlice slice;

        public SliceRestriction(CFMetaData cfm, List<ColumnDefinition> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            super(cfm, columnDefs);
            slice = TermSlice.newInstance(bound, inclusive, term);
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(slice.bound(b).bindAndGet(options));
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return slice.getFunctions();
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        protected PrimaryKeyRestrictions doMergeWith(TokenRestriction otherRestriction)
        throws InvalidRequestException
        {
            if (!otherRestriction.isSlice())
                throw invalidRequest("Columns \"%s\" cannot be restricted by both an equality and an inequality relation",
                                     getColumnNamesAsString());

            TokenRestriction.SliceRestriction otherSlice = (TokenRestriction.SliceRestriction) otherRestriction;

            if (hasBound(Bound.START) && otherSlice.hasBound(Bound.START))
                throw invalidRequest("More than one restriction was found for the start bound on %s",
                                     getColumnNamesAsString());

            if (hasBound(Bound.END) && otherSlice.hasBound(Bound.END))
                throw invalidRequest("More than one restriction was found for the end bound on %s",
                                     getColumnNamesAsString());

            return new SliceRestriction(metadata, columnDefs,  slice.merge(otherSlice.slice));
        }

        @Override
        public String toString()
        {
            return String.format("SLICE%s", slice);
        }
        private SliceRestriction(CFMetaData cfm, List<ColumnDefinition> columnDefs, TermSlice slice)
        {
            super(cfm, columnDefs);
            this.slice = slice;
        }
    }
}
