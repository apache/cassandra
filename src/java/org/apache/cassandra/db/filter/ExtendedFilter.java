package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Extends a column filter (IFilter) to include a number of IndexExpression.
 */
public abstract class ExtendedFilter
{
    private static Logger logger = LoggerFactory.getLogger(ExtendedFilter.class);

    public final ColumnFamilyStore cfs;
    protected final IFilter originalFilter;
    private final int maxResults;
    private final boolean maxIsColumns;

    public static ExtendedFilter create(ColumnFamilyStore cfs, IFilter filter, List<IndexExpression> clause, int maxResults, boolean maxIsColumns)
    {
        if (clause == null || clause.isEmpty())
            return new EmptyClauseFilter(cfs, filter, maxResults, maxIsColumns);
        else
            return new FilterWithClauses(cfs, filter, clause, maxResults, maxIsColumns);
    }

    protected ExtendedFilter(ColumnFamilyStore cfs, IFilter filter, int maxResults, boolean maxIsColumns)
    {
        assert cfs != null;
        assert filter != null;
        this.cfs = cfs;
        this.originalFilter = filter;
        this.maxResults = maxResults;
        this.maxIsColumns = maxIsColumns;
        if (maxIsColumns)
            originalFilter.updateColumnsLimit(maxResults);
    }

    public int maxRows()
    {
        return maxIsColumns ? Integer.MAX_VALUE : maxResults;
    }

    public int maxColumns()
    {
        return maxIsColumns ? maxResults : Integer.MAX_VALUE;
    }

    /**
     * Update the filter if necessary given the number of column already
     * fetched.
     */
    public void updateColumnsLimit(int columnsCount)
    {
        if (!maxIsColumns)
            return;

        int remaining = maxResults - columnsCount;
        initialFilter().updateColumnsLimit(remaining);
    }

    /** The initial filter we'll do our first slice with (either the original or a superset of it) */
    public abstract IFilter initialFilter();

    public abstract List<IndexExpression> getClause();

    /**
     * Returns a filter to query the columns from the clause that the initial slice filter may not have caught.
     * @param data the data retrieve by the initial filter
     * @return a filter or null if there can't be any columns we missed with our initial filter (typically if it was a names query, or a slice of the entire row)
     */
    public abstract IFilter getExtraFilter(ColumnFamily data);

    /**
     * @return data pruned down to the columns originally asked for
     */
    public abstract ColumnFamily prune(ColumnFamily data);

    /**
     * @return true if the provided data satisfies all the expressions from
     * the clause of this filter.
     */
    public abstract boolean isSatisfiedBy(ColumnFamily data);

    public static boolean satisfies(int comparison, IndexOperator op)
    {
        switch (op)
        {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }

    private static class FilterWithClauses extends ExtendedFilter
    {
        protected final List<IndexExpression> clause;
        protected final IFilter initialFilter;

        public FilterWithClauses(ColumnFamilyStore cfs, IFilter filter, List<IndexExpression> clause, int maxResults, boolean maxIsColumns)
        {
            super(cfs, filter, maxResults, maxIsColumns);
            assert clause != null;
            this.clause = clause;
            this.initialFilter = computeInitialFilter();
        }

        /** Sets up the initial filter. */
        private IFilter computeInitialFilter()
        {
            if (originalFilter instanceof SliceQueryFilter)
            {
                // if we have a high chance of getting all the columns in a single index slice (and it's not too costly), do that.
                // otherwise, the extraFilter (lazily created) will fetch by name the columns referenced by the additional expressions.
                if (cfs.getMaxRowSize() < DatabaseDescriptor.getColumnIndexSize())
                {
                    logger.debug("Expanding slice filter to entire row to cover additional expressions");
                    return new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                ((SliceQueryFilter) originalFilter).reversed,
                                                Integer.MAX_VALUE);
                }
            }
            else
            {
                logger.debug("adding columns to original Filter to cover additional expressions");
                assert originalFilter instanceof NamesQueryFilter;
                SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
                for (IndexExpression expr : clause)
                {
                    columns.add(expr.column_name);
                }
                if (columns.size() > 0)
                {
                    columns.addAll(((NamesQueryFilter) originalFilter).columns);
                    return new NamesQueryFilter(columns);
                }
            }
            return originalFilter;
        }

        public IFilter initialFilter()
        {
            return initialFilter;
        }

        public List<IndexExpression> getClause()
        {
            return clause;
        }

        /*
         * We may need an extra query only if the original was a slice query (and thus may have miss the expression for the clause).
         * Even then, there is no point in doing an extra query if the original filter grabbed the whole row.
         * Lastly, we only need the extra query if we haven't yet got all the expressions from the clause.
         */
        private boolean needsExtraQuery(ColumnFamily data)
        {
            if (!(originalFilter instanceof SliceQueryFilter))
                return false;

            SliceQueryFilter filter = (SliceQueryFilter)originalFilter;
            // Check if we've fetch the whole row
            if (filter.start.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
             && filter.finish.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
             && filter.count == Integer.MAX_VALUE)
                return false;

            for (IndexExpression expr : clause)
            {
                if (data.getColumn(expr.column_name) == null)
                {
                    logger.debug("adding extraFilter to cover additional expressions");
                    return true;
                }
            }
            return false;
        }

        public IFilter getExtraFilter(ColumnFamily data)
        {
            if (!needsExtraQuery(data))
                return null;

            // Note: for counters we must be careful to not add a column that was already there (to avoid overcount). That is
            // why we do the dance of avoiding to query any column we already have (it's also more efficient anyway)
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
            for (IndexExpression expr : clause)
            {
                if (data.getColumn(expr.column_name) == null)
                    columns.add(expr.column_name);
            }
            assert !columns.isEmpty();
            return new NamesQueryFilter(columns);
        }

        public ColumnFamily prune(ColumnFamily data)
        {
            if (initialFilter == originalFilter)
                return data;
            ColumnFamily pruned = data.cloneMeShallow();
            IColumnIterator iter = originalFilter.getMemtableColumnIterator(data, null);
            originalFilter.collectReducedColumns(pruned, iter, cfs.gcBefore());
            return pruned;
        }

        public boolean isSatisfiedBy(ColumnFamily data)
        {
            // We enforces even the primary clause because reads are not synchronized with writes and it is thus possible to have a race
            // where the index returned a row which doesn't have the primary column when we actually read it
            for (IndexExpression expression : clause)
            {
                // check column data vs expression
                IColumn column = data.getColumn(expression.column_name);
                if (column == null)
                    return false;
                int v = data.metadata().getValueValidator(expression.column_name).compare(column.value(), expression.value);
                if (!satisfies(v, expression.op))
                    return false;
            }
            return true;
        }
    }

    private static class EmptyClauseFilter extends ExtendedFilter
    {
        public EmptyClauseFilter(ColumnFamilyStore cfs, IFilter filter, int maxResults, boolean maxIsColumns)
        {
            super(cfs, filter, maxResults, maxIsColumns);
        }

        public IFilter initialFilter()
        {
            return originalFilter;
        }

        public List<IndexExpression> getClause()
        {
            throw new UnsupportedOperationException();
        }

        public IFilter getExtraFilter(ColumnFamily data)
        {
            return null;
        }

        public ColumnFamily prune(ColumnFamily data)
        {
            return data;
        }

        public boolean isSatisfiedBy(ColumnFamily data)
        {
            return true;
        }
    }
}
