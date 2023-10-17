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

import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.SequenceState;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;

/**
 * A sequence of transformations associated with a node. When a node is executing a sequence, the order of transformations
 * is strictly defined. The node also cannot execute any other sequences until the current one is completed.
 */
public abstract class InProgressSequence<T extends InProgressSequence<T>>
{
    public abstract InProgressSequences.Kind kind();

    public abstract ProgressBarrier barrier();

    public String status()
    {
        return "kind: " + kind() + ", next step: " + nextStep() +" barrier: " + barrier();
    }

    /**
     * Returns a kind of the next step
     */
    public abstract Transformation.Kind nextStep();

    /**
     * Executes the next step. Returns whether or the sequence can continue / retry safely. Can return
     * false in cases when bootstrap streaming failed, or when the user has requested to halt the bootstrap sequence
     * and avoid joining the ring.
     */
    public abstract SequenceState executeNext();

    /**
     * Advance the state of in-progress sequence after execution
     */
    public abstract T advance(Epoch waitForWatermark);

    // TODO rename this. It really provides the result of undoing any steps in the sequence
    //      which have already been executed
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a kind of step that follows the given in the sequence.
     */
    protected abstract Transformation.Kind stepFollowing(Transformation.Kind kind);

    protected abstract NodeId nodeId();

    protected ClusterMetadata commit(Transformation transform)
    {
        NodeId targetNode = nodeId();
        assert nextStep() == transform.kind() : String.format(String.format("Expected %s to be next step, but got %s.", nextStep(), transform.kind()));
        return ClusterMetadataService.instance().commit(transform,
                      (metadata) -> metadata,
                      (metadata, code, reason) -> {
                          InProgressSequence<?> seq = metadata.inProgressSequences.get(targetNode);
                          Transformation.Kind actual = seq == null ? null : seq.nextStep();

                          Transformation.Kind expectedNextOp = stepFollowing(transform.kind());
                          // It is possible that we have committed this transformation in our attempt to retry
                          // after a timeout. For example, if we commit START_JOIN, we would get MID_JOIN as
                          // an actual op. Since MID_JOIN is also what we expect after committing START_JOIN,
                          // we assume that we have successfully committed it.
                          if (expectedNextOp != actual)
                              throw new IllegalStateException(String.format("Expected next operation to be %s, but got %s: %s", expectedNextOp, actual, reason));

                          // Suceeded after retry
                          return metadata;
                      });
    }
}
