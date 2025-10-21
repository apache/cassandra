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

package org.apache.cassandra.service.accord.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ViolationHandler;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.utils.Invariants.illegalState;

public class AccordViolationHandler implements ViolationHandler
{
    private static final Logger logger = LoggerFactory.getLogger(AccordViolationHandler.class);

    public static void setup()
    {
        ViolationHandlerHolder.set(AccordViolationHandler::new);
    }

    @Override
    public void onTimestampViolation(SafeCommandStore safeStore, Command command, Participants<?> otherParticipants, Route<?> otherRoute, Timestamp otherExecuteAt)
    {
        throw illegalState(ViolationHandler.timestampViolationMessage(safeStore, command, otherParticipants, otherRoute, otherExecuteAt));
    }

    @Override
    public void onDependencyViolation(Participants<?> participants, TxnId notWitnessed, Timestamp notWitnessedExecuteAt, TxnId by, Timestamp byExecuteAt)
    {
        logger.error(ViolationHandler.dependencyViolationMessage(participants, notWitnessed, notWitnessedExecuteAt, by, byExecuteAt));
    }
}
