(* Licensed to the Apache Software Foundation (ASF) under one
(* or more contributor license agreements.  See the NOTICE file
(* distributed with this work for additional information
(* regarding copyright ownership.  The ASF licenses this file
(* to you under the Apache License, Version 2.0 (the
(* "License"); you may not use this file except in compliance
(* with the License.  You may obtain a copy of the License at
(*
(*     http://www.apache.org/licenses/LICENSE-2.0
(*
(* Unless required by applicable law or agreed to in writing, software
(* distributed under the License is distributed on an "AS IS" BASIS,
(* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
(* See the License for the specific language governing permissions and
(* limitations under the License.

---- MODULE Notify ----
\* Three tasks sharing one command entry and two key entries, with mixed regions
\* and mixed sync-ness: enough for a task to be demoted on the txnId while holding
\* key positions, which is the state the counters have to survive.
EXTENDS AccordNotify
MCTaskTxns   == <<{"c1"}, {"c1"}, {}>>
MCTaskKeys   == <<{"k1","k2"}, {"k1"}, {"k1","k2"}>>
MCTaskRegion == <<"ORD", "FIFO", "ORD">>
MCTaskSync   == <<TRUE, FALSE, FALSE>>
====
