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

package org.apache.cassandra.distributed.fuzz;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.model.Model;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Query;

public class QueryingNoOpChecker implements Model
{
    public static void init()
    {
        Configuration.registerSubtypes(QueryingNoOpCheckerConfig.class);
    }

    private final Run run;

    public QueryingNoOpChecker(Run run)
    {
        this.run = run;
    }

    @Override
    public void validate(Query query)
    {
        CompiledStatement compiled = query.toSelectStatement();
        run.sut.execute(compiled.cql(),
                        SystemUnderTest.ConsistencyLevel.ALL,
                        compiled.bindings());
    }

    @JsonTypeName("querying_no_op_checker")
    public static class QueryingNoOpCheckerConfig implements Configuration.ModelConfiguration
    {
        @JsonCreator
        public QueryingNoOpCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new QueryingNoOpChecker(run);
        }
    }
}
