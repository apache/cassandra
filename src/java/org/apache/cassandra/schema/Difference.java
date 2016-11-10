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
package org.apache.cassandra.schema;

public enum Difference
{
    /**
     * Two schema objects are considered to differ DEEP-ly if one or more of their nested schema objects differ.
     *
     * For example, if a table T has a column c of type U, where U is a user defined type, then upon altering U table
     * T0 (before alter) will differ DEEP-ly from table T1 (after alter).
     */
    DEEP,

    /**
     *
     * Two schema objects are considered to differ DEEP-ly if their direct structure is altered.
     *
     * For example, if a table T is altered to add a new column, a different compaction strategy, or a new description,
     * then it will differ SHALLOW-ly from the original.
     */
    SHALLOW
}
