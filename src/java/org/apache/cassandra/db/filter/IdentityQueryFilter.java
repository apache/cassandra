package org.apache.cassandra.db.filter;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.SuperColumn;

public class IdentityQueryFilter extends SliceQueryFilter
{
    /**
     * Only for use in testing; will read entire CF into memory.
     */
    public IdentityQueryFilter(String key, QueryPath path)
    {
        super(key, path, ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, Integer.MAX_VALUE);
    }

    public void filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        // no filtering done, deliberately
    }
}
