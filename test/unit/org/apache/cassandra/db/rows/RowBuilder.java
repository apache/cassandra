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

package org.apache.cassandra.db.rows;

import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Row.Builder;
import org.apache.cassandra.utils.Pair;

/**
 * Instrumented Builder implementation for testing the
 * behavior of Cells and Rows static methods
 */
public class RowBuilder implements Row.Builder
{
    public List<Cell> cells = new LinkedList<>();
    public Clustering clustering = null;
    public LivenessInfo livenessInfo = null;
    public Row.Deletion deletionTime = null;
    public List<Pair<ColumnDefinition, DeletionTime>> complexDeletions = new LinkedList<>();

    @Override
    public Builder copy()
    {
        throw new UnsupportedOperationException();
    }

    public void addCell(Cell cell)
    {
        cells.add(cell);
    }

    public boolean isSorted()
    {
        throw new UnsupportedOperationException();
    }

    public void newRow(Clustering clustering)
    {
        assert this.clustering == null;
        this.clustering = clustering;
    }

    public Clustering clustering()
    {
        return clustering;
    }

    public void addPrimaryKeyLivenessInfo(LivenessInfo info)
    {
        assert livenessInfo == null;
        livenessInfo = info;
    }

    public void addRowDeletion(Row.Deletion deletion)
    {
        assert deletionTime == null;
        deletionTime = deletion;
    }

    public void addComplexDeletion(ColumnDefinition column, DeletionTime complexDeletion)
    {
        complexDeletions.add(Pair.create(column, complexDeletion));
    }

    public Row build()
    {
        throw new UnsupportedOperationException();
    }
}
