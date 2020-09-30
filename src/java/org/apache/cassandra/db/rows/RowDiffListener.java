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

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.*;

/**
 * Interface that allows to act on the result of merging multiple rows.
 *
 * More precisely, given N rows and the result of merging them, one can call {@link Rows#diff(RowDiffListener, Row, Row...)}
 * with a {@code RowDiffListener} and that listener will be informed for each input row of the diff between
 * that input and merge row.
 */
public interface RowDiffListener
{
    /**
     * Called for the row primary key liveness info of input {@code i}.
     *
     * @param i the input row from which {@code original} is from.
     * @param clustering the clustering for the row that is merged.
     * @param merged the primary key liveness info of the merged row. Will be {@code null} if input {@code i} had
     * a {@code LivenessInfo}, but the merged result don't (i.e. the original info has been shadowed/deleted).
     * @param original the primary key liveness info of input {@code i}. May be {@code null} if input {@code i}
     * has not primary key liveness info (i.e. it has {@code LivenessInfo.NONE}) but the merged result has.
     */
    public void onPrimaryKeyLivenessInfo(int i, Clustering<?> clustering, LivenessInfo merged, LivenessInfo original);

    /**
     * Called for the row deletion of input {@code i}.
     *
     * @param i the input row from which {@code original} is from.
     * @param clustering the clustering for the row that is merged.
     * @param merged the deletion of the merged row. Will be {@code null} if input {@code i} had deletion
     * but the merged result doesn't (i.e. the deletion has been shadowed).
     * @param original the deletion of input {@code i}. May be {@code null} if input {@code i} had no deletion but the merged row has.
     */
    public void onDeletion(int i, Clustering<?> clustering, Row.Deletion merged, Row.Deletion original);

    /**
     * Called for every (non-live) complex deletion of any complex column present in either the merged row of input {@code i}.
     *
     * @param i the input row from which {@code original} is from.
     * @param clustering the clustering for the row that is merged.
     * @param column the column for which this is a complex deletion of.
     * @param merged the complex deletion of the merged row. Will be {@code null} if input {@code i} had a complex deletion
     * for {@code column} but the merged result doesn't (i.e. the deletion has been shadowed).
     * @param original the complex deletion of input {@code i} for column {@code column}. May be {@code null} if input {@code i}
     * had no complex deletion but the merged row has.
     */
    public void onComplexDeletion(int i, Clustering<?> clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original);

    /**
     * Called for any cell that is either in the merged row or in input {@code i}.
     *
     * @param i the input row from which {@code original} is from.
     * @param clustering the clustering for the row that is merged.
     * @param merged the cell of the merged row. Will be {@code null} if input {@code i} had a cell but that cell is no present
     * in the merged result (it has been deleted/shadowed).
     * @param original the cell of input {@code i}. May be {@code null} if input {@code i} had cell corresponding to {@code merged}.
     */
    public void onCell(int i, Clustering<?> clustering, Cell<?> merged, Cell<?> original);
}
