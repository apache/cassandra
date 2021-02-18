/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.analyzer.filter;

/**
 * Executes all linked Pipeline Tasks serially and returns
 * output (if exists) from the executed logic
 */
public class FilterPipelineExecutor
{
    public static String execute(FilterPipelineTask task, String initialInput)
    {
        FilterPipelineTask taskPtr = task;
        String result = initialInput;
        
        while (true)
        {
            FilterPipelineTask taskGeneric = taskPtr;
            result = taskGeneric.process(result);
            taskPtr = taskPtr.next;
            
            if (taskPtr == null)
            {
                return result;
            }
        }
    }
}
