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

/**
 * The Simulator is a facility for deterministic pseudorandom execution of anything
 * from a snippet of code to an entire Cassandra cluster. A simulation execution
 * consists of an {@link org.apache.cassandra.simulator.ActionPlan} containing various
 * {@link org.apache.cassandra.simulator.Action} to simulate. Each {@code Action} represents
 * some discrete unit of simulation and its consequences - which are themselves {@code Action}s.
 * A simulation completes once all transitive consequences of the initial actions have completed.
 * The execution order is determined by an {@link org.apache.cassandra.simulator.ActionSchedule}
 * and its {@link org.apache.cassandra.simulator.RunnableActionScheduler} and
 * {@link org.apache.cassandra.simulator.FutureActionScheduler}.
 *
 * An {@code Action} may be simple, representing some simulation book-keeping or setup, but most
 * commonly derives from {@link org.apache.cassandra.simulator.systems.SimulatedAction}.
 * These simulated actions represent some execution context on an {@link org.apache.cassandra.simulator.systems.InterceptibleThread},
 * such as a task execution on an executor or a new thread. An {@code Action} is typically a one-shot
 * concept, but these threaded actions simultaneously represent any initial synchronous step taken
 * by a threaded execution context and the continuation of that state - through the creation of new
 * child and continuation {@code Action} that resume execution as the schedule demands. Note that such
 * a threaded action may have no consequences while still being in progress, e.g. when the thread enters
 * an unbounded wait condition, in which case it expects to be awoken by some other threaded action's
 * interaction with it.
 *
 * {@code Action} have various causality mechanisms to control their behaviour, in the form of {@link org.apache.cassandra.simulator.Action.Modifier}
 * options that may be applied to themselves, transitively to all of their descendants, or to specific verbs that are
 * consequences of a {@code SimulatedAction}. Also in the form of {@link org.apache.cassandra.simulator.OrderOn}
 * constraints that may force actions to occur in strictly sequential order, or simply to occur at some maximum rate.
 * This latter facility supports both the simulation of executor services and constraints for a correct simulation e.g.
 * when some action's children must occur in sequential order to correctly simulate some system operation.
 *
 * Simulation is achieved through various simulated systems ({@link org.apache.cassandra.simulator.systems})
 * and byte weaving ({@link org.apache.cassandra.simulator.asm}) of implementation classes, and chaos is introduced
 * via {@link org.apache.cassandra.simulator.cluster.KeyspaceActions} and also {@link org.apache.cassandra.utils.Nemesis}
 * points and on monitor entry and exit. Collectively these systems take control of: monitors, lock support, blocking
 * data structures, threads and executors, random number generation, time, paxos ballots, the network and failure detection.
 *
 * The first major simulation introduced was {@link org.apache.cassandra.simulator.paxos.PaxosSimulationRunner} which
 * may be invoked on the command line to simulate a cluster and some proportion of LWT read/write operations.
 *
 * For simulator simulation to operate correctly, be sure to run {@code ant simulator-jars} and to include the
 * following JVM parameters:
 *    -javaagent:/path/to/project.dir/build/test/lib/simulator-asm.jar
 *    -Xbootclasspath/a:/path/to/project.dir/build/test/lib/simulator-bootstrap.jar
 *    -Dlogback.configurationFile=file:///path/to/project.dir/test/conf/logback-simulator.xml
 *
 * For a reproducible simulation, make sure to use the same JVM and JDK for all runs, and to supply the following
 * JVM parameters:
 *    -XX:ActiveProcessorCount=???
 *    -Xmx???
 *    -XX:-BackgroundCompilation
 *    -XX:CICompilerCount=1
 *
 * For performance reasons the following parameters are recommended:
 *    -XX:Tier4CompileThreshold=1000
 *    -XX:-TieredCompilation
 *    -XX:ReservedCodeCacheSize=256M
 *
 * When debugging, the following parameters are recommended:
 *    -Dcassandra.test.simulator.livenesscheck=false
 *    -Dcassandra.test.simulator.debug=true
 *
 * Note also that when debugging the execution of arbitrary methods may modify the program output if these methods
 * invoke any simulated methods, notably if the method is synchronised or otherwise uses a monitor, or invokes
 * {@code System.identityHashCode} or a non-overidden {@code Object.hashCode()}, or many other scenarios.
 * To support debugging, this behaviour is switched off as far as possible when evaluating any {@code toString()}
 * method, but it is worth being prepared for the possibility that your use of the debugger has the potential to
 * perturb program execution.
 */
package org.apache.cassandra.simulator;