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
package org.apache.cassandra;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * This class comes useful when debugging flaky tests that will fail only when the full test suite is ran. It is
 * intended for test failure investigation only. Decorate your class with the runner and hardcode the iterations you
 * need.
 * 
 * If your test uses a runner already, as they can't be chained, you can use this class as inspiration to modify the
 * original runner to loop your test.
 */
public class RepeatableRunner extends BlockJUnit4ClassRunner
{
    public RepeatableRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
    }

    @Override
    public void run(RunNotifier notifier)
    {
        for (int i = 0; i < 10; i++)
        {
            super.run(notifier);
        }
    }
}
