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

package org.apache.cassandra.simulator.systems;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A simple encapsulation for capturing and reporting failures during the simulation
 */
public class Failures implements Consumer<Throwable>
{
    private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean hasFailure;

    public void onFailure(Throwable t)
    {
        failures.add(t);
        hasFailure = true;
    }

    public boolean hasFailure()
    {
        return hasFailure;
    }

    public List<Throwable> get()
    {
        return Collections.unmodifiableList(failures);
    }

    @Override
    public void accept(Throwable throwable)
    {
        onFailure(throwable);
    }
}
