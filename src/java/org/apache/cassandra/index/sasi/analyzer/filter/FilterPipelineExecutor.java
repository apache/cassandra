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
package org.apache.cassandra.index.sasi.analyzer.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes all linked Pipeline Tasks serially and returns
 * output (if exists) from the executed logic
 */
public class FilterPipelineExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(FilterPipelineExecutor.class);

    public static <F,T> T execute(FilterPipelineTask<F, T> task, T initialInput)
    {
        FilterPipelineTask<?, ?> taskPtr = task;
        T result = initialInput;
        try
        {
            while (true)
            {
                FilterPipelineTask<F,T> taskGeneric = (FilterPipelineTask<F,T>) taskPtr;
                result = taskGeneric.process((F) result);
                taskPtr = taskPtr.next;
                if(taskPtr == null)
                    return result;
            }
        }
        catch (Exception e)
        {
            logger.info("An unhandled exception to occurred while processing " +
                    "pipeline [{}]", task.getName(), e);
        }
        return null;
    }
}
