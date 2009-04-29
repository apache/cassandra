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

import org.apache.log4j.Logger;

/**
 * This class represents the execution plan for Query (data retrieval) statement. 
 */
public class QueryPlan extends Plan
{
    private final static Logger logger_ = Logger.getLogger(QueryPlan.class);    

    public RowSourceDef root;    // the root of the row source tree

    public QueryPlan(RowSourceDef rwsDef)
    {
        root = rwsDef;
    }
    
    public CqlResult execute()
    {
        if (root != null)
        {
            return new CqlResult(root.getRows());
        }
        else
            logger_.error("No rowsource to execute");
        return null;
    }
    
    public String explainPlan()
    {
        return root.explainPlan();
    }
    
}
