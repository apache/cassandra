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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;

import accord.utils.Functions;
import accord.utils.Invariants;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static accord.utils.Functions.anyMatches;

public final class Operations implements Iterable<Operation>
{
    private final StatementType type;
    private final List<Operation> regularOps = new ArrayList<>();
    private final List<Operation> staticOps = new ArrayList<>();
    private final List<ReferenceOperation> regularRefOps = new ArrayList<>();
    private final List<ReferenceOperation> staticRefOps = new ArrayList<>();

    public Operations(StatementType type)
    {
        this.type = type;
    }

    public Operations asTxnCompatible(TableMetadata tableMetadata)
    {
        if (isTxnCompatible())
            return this;

        Invariants.require(!hasRefOps(), "Already has transaction compatible operations, should not be partially compatible");
        Operations result = new Operations(type);
        for (int i = 0, maxi = staticOps.size() ; i < maxi ; i++)
            result.add(staticOps.get(i), tableMetadata, true);
        for (int i = 0, maxi = regularOps.size() ; i < maxi ; i++)
            result.add(regularOps.get(i), tableMetadata, true);
        return result;
    }

    /**
     * Checks if some of the operations apply to static columns.
     *
     * @return <code>true</code> if some of the operations apply to static columns, <code>false</code> otherwise.
     */
    public boolean appliesToStaticColumns()
    {
        return !staticIsEmpty();
    }

    /**
     * Checks if some of the operations apply to regular columns.
     *
     * @return <code>true</code> if some of the operations apply to regular columns, <code>false</code> otherwise.
     */
    public boolean appliesToRegularColumns()
    {
        // If we have regular operations, this applies to regular columns.
        // Otherwise, if the statement is a DELETE and staticOperations is also empty, this means we have no operations,
        // which for a DELETE means a full row deletion. Which means the operation applies to all columns and regular ones in particular.
        return !regularIsEmpty() || (type.isDelete() && staticIsEmpty());
    }

    /**
     * Returns the operation on regular columns.
     * @return the operation on regular columns
     */
    public List<Operation> regularOperations()
    {
        return regularOps;
    }

    /**
     * Returns the operation on static columns.
     * @return the operation on static columns
     */
    public List<Operation> staticOperations()
    {
        return staticOps;
    }

    /**
     * Adds the specified <code>Operation</code> to this set of operations.
     *
     * @param operation     the operation to add
     * @param tableMetadata
     */
    public void add(Operation operation, TableMetadata tableMetadata, boolean isForTxn)
    {
        if (isForTxn && (operation.requiresRead() || operation.requiresTimestamp()))
            add(operation.column, ReferenceOperation.create(operation, tableMetadata));
        else if (operation.column.isStatic())
            staticOps.add(operation);
        else
            regularOps.add(operation);
    }

    public void add(ColumnMetadata column, ReferenceOperation operation)
    {
        if (column.isStatic())
            staticRefOps.add(operation);
        else
            regularRefOps.add(operation);
    }

    /**
     * Checks if one of the operations requires a read.
     *
     * @return <code>true</code> if one of the operations requires a read, <code>false</code> otherwise.
     */
    public boolean requiresRead()
    {
        // Lists SET operation incurs a read.
        for (Operation operation : this)
            if (operation.requiresRead())
                return true;

        return false;
    }

    /**
     * Return false if an operation must be evaluated/bound at execution time.
     * i.e. if it reads-before-writes or relies on the execution timestamp (which is effectively a special case of read-before-write)
     */
    public boolean isTxnCompatible()
    {
        return !anyOpMatches(Operation::requiresRead) && !anyOpMatches(Operation::requiresTimestamp);
    }

    /**
     * Checks if this <code>Operations</code> is empty.
     * @return <code>true</code> if this <code>Operations</code> is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty()
    {
        return staticIsEmpty() && regularIsEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Operation> iterator()
    {
        return Iterators.concat(staticOps.iterator(), regularOps.iterator());
    }

    public void addFunctionsTo(List<Function> functions)
    {
        Functions.forEach(Operation::addFunctionsTo, regularOps, functions);
        Functions.forEach(Operation::addFunctionsTo, staticOps, functions);
        // refOps don't support functions
    }

    public Collection<ReferenceOperation> allRefOps()
    {
        if (staticRefOps.isEmpty())
            return regularRefOps;
        
        if (regularRefOps.isEmpty())
            return staticRefOps;

        // Only create a new list if we actually have something to combine
        return new AbstractCollection<>()
        {
            @Override public Iterator<ReferenceOperation> iterator() { return Iterators.concat(staticRefOps.iterator(), regularRefOps.iterator()); }
            @Override public int size() { return staticRefOps.size() + regularRefOps.size(); }
        };
    }

    public boolean anyOpMatches(Predicate<Operation> test)
    {
        return anyMatches(test, staticOps) || anyMatches(test, regularOps);
    }

    public boolean anyRefOpMatches(Predicate<ReferenceOperation> test)
    {
        return anyMatches(test, staticRefOps) || anyMatches(test, regularRefOps);
    }


    public boolean hasRefOps()
    {
        return !staticRefOps.isEmpty() || !regularRefOps.isEmpty();
    }

    public List<ReferenceOperation> regularRefOps()
    {
        return regularRefOps;
    }

    public List<ReferenceOperation> staticRefOps()
    {
        return staticRefOps;
    }

    private boolean regularIsEmpty()
    {
        return regularOps.isEmpty() && regularRefOps.isEmpty();
    }

    private boolean staticIsEmpty()
    {
        return staticOps.isEmpty() && staticRefOps.isEmpty();
    }
}
