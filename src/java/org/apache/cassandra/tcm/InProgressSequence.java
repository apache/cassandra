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

package org.apache.cassandra.tcm;

import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;

public interface InProgressSequence<T extends InProgressSequence<T>>
{
    InProgressSequences.Kind kind();

    ProgressBarrier barrier();

    default String status()
    {
        return "kind: " + kind() + ", next step: " + nextStep() +" barrier: " + barrier();
    }

    /**
     * Returns a kind of the next step
     */
    public Transformation.Kind nextStep();

    /**
     * Executes the next step. Returns whether or the sequence can continue / retry safely. Can return
     * false in cases when bootstrap streaming failed, or when the user has requested to halt the bootstrap sequence
     * and avoid joining the ring.
     */
    public boolean executeNext();

    public T advance(Epoch waitForWatermark, Transformation.Kind next);

    // TODO rename this. It really provides the result of undoing any steps in the sequence
    //      which have already been executed
    default ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }
}
