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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.visitors.AllPartitionsValidator;
import org.apache.cassandra.harry.visitors.QueryLogger;
import org.apache.cassandra.harry.visitors.Visitor;

/**
 * Validator similar to {@link AllPartitionsValidator}, but performs
 * repair before checking node states.
 *
 * Can be useful for testing repair, bootstrap, and streaming code.
 */
public class RepairingLocalStateValidator extends AllPartitionsValidator
{
    private static final Logger logger = LoggerFactory.getLogger(RepairingLocalStateValidator.class);

    private final InJvmSut inJvmSut;

    public static Configuration.VisitorConfiguration factoryForTests(int concurrency, Model.ModelFactory modelFactory)
    {
        return (r) -> new RepairingLocalStateValidator(r, concurrency, modelFactory);
    }

    public RepairingLocalStateValidator(Run run, int concurrency, Model.ModelFactory modelFactory)
    {
        this(run, concurrency, modelFactory, QueryLogger.NO_OP);
    }

    public RepairingLocalStateValidator(Run run, int concurrency, Model.ModelFactory modelFactory, QueryLogger queryLogger)
    {
        super(run, concurrency, modelFactory, queryLogger);
        this.inJvmSut = (InJvmSut) run.sut;
    }

    public void visit()
    {
        logger.debug("Starting repair...");
        inJvmSut.cluster().stream().forEach((instance) -> instance.nodetool("repair", "--full"));
        logger.debug("Validating partitions...");
        super.visit();
    }

    @JsonTypeName("repair_and_validate_local_states")
    public static class RepairingLocalStateValidatorConfiguration implements Configuration.VisitorConfiguration
    {
        private final int concurrency;
        private final Configuration.ModelConfiguration modelConfiguration;
        private final Configuration.QueryLoggerConfiguration query_logger;

        @JsonCreator
        public RepairingLocalStateValidatorConfiguration(@JsonProperty("concurrency") int concurrency,
                                                         @JsonProperty("model") Configuration.ModelConfiguration model,
                                                         @JsonProperty("query_logger") Configuration.QueryLoggerConfiguration query_logger)
        {
            this.concurrency = concurrency;
            this.modelConfiguration = model;
            this.query_logger = QueryLogger.thisOrDefault(query_logger);
        }

        public Visitor make(Run run)
        {
            return new RepairingLocalStateValidator(run, concurrency, modelConfiguration, query_logger.make());
        }
    }
}
