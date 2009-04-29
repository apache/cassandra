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

import java.util.*;
import org.apache.log4j.Logger;

/**
 * The "Plan" for the EXPLAIN PLAN statement itself!
 * 
 * It is nothing but a simple wrapper around the "Plan" for the statement
 * on which an EXPLAIN PLAN has been requested.
 */
public class ExplainPlan extends Plan
{
    private final static Logger logger_ = Logger.getLogger(ExplainPlan.class);
    
    // the execution plan for the statement on which an 
    // EXPLAIN PLAN was requested.
    private Plan plan_ = null;

    /**
     *  Construct an ExplainPlan instance for the statement whose
     * "plan" has been passed in.
     */
    public ExplainPlan(Plan plan)
    {
        plan_ = plan;
    }

    public CqlResult execute()
    {
        String planText = plan_.explainPlan();

        List<Map<String, String>> rows = new LinkedList<Map<String, String>>(); 
        Map<String, String> row = new HashMap<String, String>();
        row.put("PLAN", planText);
        rows.add(row);
        
        return new CqlResult(rows);
    }

    public String explainPlan()
    {
        // We never expect this method to get invoked for ExplainPlan instances
        // (i.e. those that correspond to the EXPLAIN PLAN statement).
        logger_.error("explainPlan() invoked on an ExplainPlan instance");
        return null;
    }
}