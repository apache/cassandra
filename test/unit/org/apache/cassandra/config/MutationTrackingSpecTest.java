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

package org.apache.cassandra.config;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MutationTrackingSpecTest
{
    @Test
    public void testValidation()
    {
        MutationTrackingSpec spec = new MutationTrackingSpec();

        spec.validate(); // defaults

        spec.journal_compaction_max_segments = 1;
        spec.validate();

        spec.journal_compaction_max_segments = Integer.MAX_VALUE;
        spec.validate();

        // 0 would truncate the whole candidate list away every pass, silently disabling segment dropping
        spec.journal_compaction_max_segments = 0;
        assertThatThrownBy(spec::validate).isInstanceOf(ConfigurationException.class)
                                          .hasMessage("Invalid value for mutation_tracking.journal_compaction_max_segments " +
                                                      "\"0\". Value must be a positive integer");

        // a negative value throws out of every pass
        spec.journal_compaction_max_segments = -1;
        assertThatThrownBy(spec::validate).isInstanceOf(ConfigurationException.class)
                                          .hasMessageContaining("Invalid value for mutation_tracking.journal_compaction_max_segments " +
                                                                "\"-1\". Value must be a positive integer");
    }
}
