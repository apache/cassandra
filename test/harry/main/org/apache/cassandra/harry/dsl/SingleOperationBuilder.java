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

package org.apache.cassandra.harry.dsl;

public interface SingleOperationBuilder
{
    /**
     * Perform an insert operation to _some_ row
     */
    SingleOperationBuilder insert();

    /**
     * Perform an insert operation to a _specific_ row. Rows are ordered by clustering key and
     * numbered from 0 to maxRows
     */
    SingleOperationBuilder insert(int rowIdx);

    /**
     * Insert _specific values_ into _specific_ row. Rows are ordered by clustering key and
     * numbered from 0 to maxRows
     */
    SingleOperationBuilder insert(int rowIdx, long[] valueIdxs);
    SingleOperationBuilder insert(int rowIdx, long[] valueIdxs, long[] sValueIdxs);

    SingleOperationBuilder deletePartition();

    SingleOperationBuilder deleteRow();
    SingleOperationBuilder deleteRow(int rowIdx);

    SingleOperationBuilder deleteColumns();

    SingleOperationBuilder deleteRowRange();
    SingleOperationBuilder deleteRowRange(int lowBoundRowIdx, int highBoundRowIdx, boolean isMinEq, boolean isMaxEq);

    SingleOperationBuilder deleteRowSlice();
}
