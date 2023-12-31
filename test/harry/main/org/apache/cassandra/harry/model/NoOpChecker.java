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

import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.operations.Query;

public class NoOpChecker implements Model
{
    private final Run run;

    public NoOpChecker(Run run)
    {
        this.run = run;
    }

    public void validate(Query query)
    {
        run.sut.execute(query.toSelectStatement(),
                        // TODO: make it configurable
                        SystemUnderTest.ConsistencyLevel.QUORUM);
    }
}
