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

package org.apache.cassandra.tools.nodetool;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.apache.cassandra.config.DurationSpec;
import org.mockito.Mock;
import org.apache.cassandra.tools.NodeProbe;
import org.mockito.MockitoAnnotations;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetValueForConfigTest
{
    @Mock
    private NodeProbe probe;
    private SetValueForConfig cmd;


    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        cmd = new SetValueForConfig();
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_ms() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20ms".split(" "));
        cmd.execute(probe);
        verify(probe, times(1)).setValueForConfig("xyz",
                                                  new DurationSpec.LongMillisecondsBound(20));
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_ns() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20ns".split(" "));
        cmd.execute(probe);
        verify(probe, times(1)).setValueForConfig("xyz",
                                                  new DurationSpec.LongNanosecondsBound(20));
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_s() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20s".split(" "));
        cmd.execute(probe);
        verify(probe, times(1)).setValueForConfig("xyz",
                                                  new DurationSpec.LongSecondsBound(20));
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_m() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20m".split(" "));
        cmd.execute(probe);
        verify(probe, times(1)).setValueForConfig("xyz",
                                                  new DurationSpec.IntMinutesBound(20));
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_h() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20h".split(" "));
        cmd.execute(probe);
        verify(probe, times(1)).setValueForConfig("xyz",
                                                  new DurationSpec.LongMillisecondsBound(20, TimeUnit.HOURS));
    }

    @Test
    public void testSetValueForConfigWithDurationSpec_invalid() {
        when(probe.setValueForConfig(anyString(), any())).thenReturn("");
        cmd.args = Arrays.asList("xyz durationspec 20x".split(" "));
        cmd.execute(probe);
        verify(probe, never()).setValueForConfig(eq("xyz"), any());
    }
}