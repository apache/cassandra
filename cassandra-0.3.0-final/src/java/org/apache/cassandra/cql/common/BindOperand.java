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

import org.apache.cassandra.cql.execution.RuntimeErrorMsg;

/**
 * BindOperand: 
 * Represents a bind variable in the CQL statement. Lives
 * in the shared execution plan.
 */
public class BindOperand implements OperandDef 
{
    int bindIndex_;  // bind position

    public BindOperand(int bindIndex)
    {
        bindIndex_ = bindIndex;
    }

    public Object get()
    {
        // TODO: Once bind variables are supported, the get() will extract
        // the value of the bind at position "bindIndex_" from the execution
        // context.
        throw new RuntimeException(RuntimeErrorMsg.IMPLEMENTATION_RESTRICTION
                                   .getMsg("bind params not yet supported"));
    }
    
    public String explain()
    {
        return "Bind #: " + bindIndex_;
    }

};