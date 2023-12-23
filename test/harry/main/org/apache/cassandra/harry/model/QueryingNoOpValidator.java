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

package org.apache.cassandra.harry.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.Query;

/**
 * A model that can be used to "simply" run random queries. Does not perform validation
 * of the results that it sees. Useful for increasing concurrency and triggering
 * exceptions rather than data loss issues.
 */
public class QueryingNoOpValidator implements Model
{
    private final Run run;

    public QueryingNoOpValidator(Run run)
    {
        this.run = run;
    }

    @Override
    public void validate(Query query)
    {
        CompiledStatement compiled = query.toSelectStatement();
        run.sut.execute(compiled.cql(),
                        SystemUnderTest.ConsistencyLevel.QUORUM,
                        compiled.bindings());
    }

    @JsonTypeName("no_op")
    public static class QueryingNoOpCheckerConfig implements Configuration.ModelConfiguration
    {
        @JsonCreator
        public QueryingNoOpCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new QueryingNoOpValidator(run);
        }
    }
}
