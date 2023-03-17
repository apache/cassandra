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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A set of <code>Operation</code>s.
 *
 */
public final class Operations implements Iterable<Operation>
{
    /**
     * The type of statement.
     */
    private final StatementType type;
    /**
     * If this operation is for a Transaction; this causes Operations to "migrate" when they require-read
     */
    private final boolean isForTxn;

    /**
     * The operations on regular columns.
     */
    private final List<Operation> regularOperations = new ArrayList<>();

    /**
     * The operations on static columns.
     */
    private final List<Operation> staticOperations = new ArrayList<>();

    private final List<ReferenceOperation> regularSubstitutions = new ArrayList<>();
    private final List<ReferenceOperation> staticSubstitutions = new ArrayList<>();

    public Operations(StatementType type, boolean isForTxn)
    {
        this.type = type;
        this.isForTxn = isForTxn;
    }

    private Operations(Operations other)
    {
        Preconditions.checkState(!other.isForTxn, "Unable to migrate from txn to txn");
        Preconditions.checkState(other.regularSubstitutions.isEmpty() && other.staticSubstitutions.isEmpty(), "Transaction substitutions are defined for a non-transaction operations! regular=%s, static=%s", other.regularSubstitutions, other.staticSubstitutions);

        type = other.type;
        isForTxn = true;
        for (Operation opt : other)
            add(opt);
    }

    public Operations forTxn()
    {
        return new Operations(this);
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
        return regularOperations;
    }

    /**
     * Returns the operation on static columns.
     * @return the operation on static columns
     */
    public List<Operation> staticOperations()
    {
        return staticOperations;
    }

    /**
     * Adds the specified <code>Operation</code> to this set of operations.
     * @param operation the operation to add
     */
    public void add(Operation operation)
    {
        if (isForTxn && operation.requiresRead())
        {
            add(operation.column, ReferenceOperation.create(operation));
            return;
        }
        if (operation.column.isStatic())
            staticOperations.add(operation);
        else
            regularOperations.add(operation);
    }

    public void add(ColumnMetadata column, ReferenceOperation operation)
    {
        Preconditions.checkState(isForTxn, "Unable to add a transaction reference to a non-transaction operation");
        if (column.isStatic())
            staticSubstitutions.add(operation);
        else
            regularSubstitutions.add(operation);
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
        return Iterators.concat(staticOperations.iterator(), regularOperations.iterator());
    }

    public void addFunctionsTo(List<Function> functions)
    {
        regularOperations.forEach(p -> p.addFunctionsTo(functions));
        staticOperations.forEach(p -> p.addFunctionsTo(functions));
        //TODO substitutions as well?
    }

    public List<ReferenceOperation> allSubstitutions()
    {
        if (staticSubstitutions.isEmpty())
            return regularSubstitutions;
        
        if (regularSubstitutions.isEmpty())
            return staticSubstitutions;

        // Only create a new list if we actually have something to combine
        List<ReferenceOperation> list = new ArrayList<>(staticSubstitutions.size() + regularSubstitutions.size());
        list.addAll(staticSubstitutions);
        list.addAll(regularSubstitutions);
        return list;
    }

    public List<ReferenceOperation> regularSubstitutions()
    {
        return regularSubstitutions;
    }

    public List<ReferenceOperation> staticSubstitutions()
    {
        return staticSubstitutions;
    }

    private boolean regularIsEmpty()
    {
        return regularOperations.isEmpty() && regularSubstitutions.isEmpty();
    }

    private boolean staticIsEmpty()
    {
        return staticOperations.isEmpty() && staticSubstitutions.isEmpty();
    }
}
