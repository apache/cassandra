/**
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

package org.apache.cassandra.cql.common;

import java.util.List;
import java.util.Map;

/**
 * The abstract notion of a row source definition. A row source
 * is literally just anything that returns rows back.
 * 
 * The concrete implementations of row source might be things like a 
 * column family row source, a "super column family" row source, 
 * a table row source, etc.
 *
 * Note: Instances of sub-classes of this class are part of the "shared" 
 * execution plan of CQL. And hence they should not contain any mutable
 * (i.e. session specific) execution state. Mutable state, such a bind
 * variable values (corresponding to say a rowKey or a column Key) are
 * note part of the RowSourceDef tree.
 * 
 * [Eventually the notion of a "mutable" portion of the RowSource (RowSourceMut)
 * will be introduced to hold session-specific execution state of the RowSource.
 * For example, this would be needed when implementing iterator style rowsources
 * that yields rows back one at a time as opposed to returning them in one
 * shot.]
 */
public abstract class RowSourceDef
{
    public abstract List<Map<String,String>> getRows();
    public abstract String explainPlan();  
}