/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

/**
 * An interface implemented to kill the JVM abnormally.
 *
 * It's currently implemented by {@link JVMStabilityInspector.Killer}
 * or tests.
 */
public interface JVMKiller
{
    /**
     * Kills the JVM in a verbose fashion.
     *
     * @param t - the error that has caused the JVM to be killed
     */
    default void killJVM(Throwable t)
    {
        killJVM(t, false);
    }

    /**
     * Kills the JVM.
     *
     * @param t - the error that has caused the JVM to be killed
     * @param quiet - whether the error should be logged verbosely
     */
    public void killJVM(Throwable t, boolean quiet);
}
