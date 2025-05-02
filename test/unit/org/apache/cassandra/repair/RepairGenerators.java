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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.List;

import accord.utils.Gen;
import accord.utils.Gens;

public class RepairGenerators
{
    public static final List<String> LOCAL_RANGE = List.of();
    public static final List<String> PRIMARY_RANGE = List.of("-pr"); // repair calls this partition range, but StorageService calls this primary

    public enum RepairType
    {
        FULL("--full"),
        IR("");

        public final String arg;

        RepairType(String s)
        {
            this.arg = s;
        }
    }

    public enum PreviewType
    {
        NONE(""),
        REPAIRED("--validate"),
        UNREPAIRED("--preview");

        public final String arg;

        PreviewType(String s)
        {
            this.arg = s;
        }
    }

    public static boolean isPreview(List<String> args)
    {
        return args.stream().anyMatch(s -> PreviewType.REPAIRED.arg.equals(s)
                                           || PreviewType.UNREPAIRED.arg.equals(s));
    }

    public static PreviewType previewType(List<String> args)
    {
        for (String s : args)
        {
            if (PreviewType.REPAIRED.arg.equals(s))
                return PreviewType.REPAIRED;
            if (PreviewType.UNREPAIRED.arg.equals(s))
                return PreviewType.UNREPAIRED;
        }
        return PreviewType.NONE;
    }

    public static boolean isFull(List<String> args)
    {
        return args.stream().anyMatch(s -> RepairType.FULL.arg.equals(s));
    }

    public static boolean isIncremental(List<String> args)
    {
        return !isFull(args);
    }


    public static class Builder
    {
        final Gen<List<String>> tablesGen;
        Gen<RepairType> typeGen = Gens.enums().all(RepairType.class);
        Gen<PreviewType> previewTypeGen = Gens.enums().all(PreviewType.class);
        Gen<List<String>> ranges = Gens.pick(List.of(), PRIMARY_RANGE);
        Gen<Boolean> optimizeStreamsGen = Gens.bools().all();
        Gen<RepairParallelism> parallelismGen = Gens.enums().all(RepairParallelism.class);
        Gen<Boolean> skipPaxosGen = i -> false;
        Gen<Boolean> skipAccordGen = i -> false;

        public Builder(Gen<List<String>> tablesGen)
        {
            this.tablesGen = tablesGen;
        }

        public Builder withType(Gen<RepairType> typeGen)
        {
            this.typeGen = typeGen;
            return this;
        }

        public Builder withPreviewType(Gen<PreviewType> previewTypeGen)
        {
            this.previewTypeGen = previewTypeGen;
            return this;
        }

        public Builder withRanges(Gen<List<String>> ranges)
        {
            this.ranges = ranges;
            return this;
        }

        public Builder withOptimizeStreams(Gen<Boolean> optimizeStreamsGen)
        {
            this.optimizeStreamsGen = optimizeStreamsGen;
            return this;
        }

        public Builder withParallelism(Gen<RepairParallelism> parallelismGen)
        {
            this.parallelismGen = parallelismGen;
            return this;
        }

        public Builder withSkipPaxosGen(Gen<Boolean> skipPaxosGen)
        {
            this.skipPaxosGen = skipPaxosGen;
            return this;
        }

        public Builder withSkipAccordGen(Gen<Boolean> skipAccordGen)
        {
            this.skipAccordGen = skipAccordGen;
            return this;
        }

        public Gen<List<String>> build()
        {
            return rs -> {
                RepairType type = typeGen.next(rs);
                PreviewType previewType = previewTypeGen.next(rs);
                List<String> args = new ArrayList<>();
                args.addAll(tablesGen.next(rs));
                args.addAll(ranges.next(rs));
                if (skipPaxosGen.next(rs))
                    args.add("--skip-paxos");
                if (skipAccordGen.next(rs))
                    args.add("--skip-accord");
                switch (type)
                {
                    case IR:
                        // default
                        break;
                    case FULL:
                        args.add(type.arg);
                        break;
                    default:
                        throw new AssertionError("Unsupported repair type: " + type);
                }
                switch (previewType)
                {
                    case NONE:
                        break;
                    case REPAIRED:
                    case UNREPAIRED:
                        args.add(previewType.arg);
                        break;
                    default:
                        throw new AssertionError("Unsupported preview type: " + previewType);
                }
                RepairParallelism parallelism = parallelismGen.next(rs);
                switch (parallelism)
                {
                    case SEQUENTIAL:
                        args.add("--sequential");
                        break;
                    case PARALLEL:
                        // default
                        break;
                    case DATACENTER_AWARE:
                        args.add("--dc-parallel");
                        break;
                    default:
                        throw new AssertionError("Unknown parallelism: " + parallelism);
                }
                if (optimizeStreamsGen.next(rs))
                    args.add("--optimise-streams");
                return args;
            };
        }
    }
}
