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

package org.apache.cassandra.schema;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlushCompressionParamsTest
{
    @Test
    public void defaultIsAuto()
    {
        assertThat(FlushCompressionParams.DEFAULT.configurationKey).isEqualTo(FlushCompressionParams.Option.auto);
        assertThat(FlushCompressionParams.DEFAULT.toString()).isEqualTo("auto");
    }

    @Test
    public void fromStringAcceptsEveryOption()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            FlushCompressionParams params = FlushCompressionParams.fromString(option.name());
            assertThat(params.configurationKey).isEqualTo(option);
            assertThat(params.toString()).isEqualTo(option.name());
        }
    }

    @Test
    public void toStringRoundTripsThroughFromString()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            FlushCompressionParams params = FlushCompressionParams.fromString(option.name());
            assertThat(FlushCompressionParams.fromString(params.toString())).isEqualTo(params);
        }
    }

    @Test
    public void fromStringRejectsUnknownValue()
    {
        assertThatThrownBy(() -> FlushCompressionParams.fromString("bogus"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Invalid value used for flush compression parameter: bogus")
        .hasMessageContaining("auto")
        .hasMessageContaining("none")
        .hasMessageContaining("fast")
        .hasMessageContaining("table");
    }

    @Test
    public void fromStringRejectsEmptyAndNull()
    {
        assertThatThrownBy(() -> FlushCompressionParams.fromString("")).isInstanceOf(ConfigurationException.class);
        assertThatThrownBy(() -> FlushCompressionParams.fromString(null)).isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void fromStringRejectsSurroundingWhitespace()
    {
        assertThatThrownBy(() -> FlushCompressionParams.fromString(" none")).isInstanceOf(ConfigurationException.class);
        assertThatThrownBy(() -> FlushCompressionParams.fromString("none ")).isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void equalityIsBasedOnOption()
    {
        FlushCompressionParams a = FlushCompressionParams.fromString("none");
        FlushCompressionParams b = FlushCompressionParams.fromString("none");
        FlushCompressionParams c = FlushCompressionParams.fromString("fast");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(null);
        assertThat(a).isNotEqualTo("none");
        assertThat(FlushCompressionParams.fromString("auto")).isEqualTo(FlushCompressionParams.DEFAULT);
    }
}
