<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

# Replacing the Java SecurityManager

## Problem

The Java `SecurityManager` is deprecated for removal (JEP 411) and is permanently disabled on
recent JDKs (JEP 486, JDK 24+). On those JDKs `System.setSecurityManager` throws
`UnsupportedOperationException`, so no `SecurityManager` can ever be installed, and several APIs
that were built on the access-control machinery either throw or have become no-ops. Cassandra used
the `SecurityManager` (directly or indirectly) in four places. Each had to be replaced by a
mechanism that provides the same guarantee without relying on an installable `SecurityManager`,
while still compiling and running across the supported JDKs (11, 17, 21, 25).

Cassandra is compiled with `source/target 11`, so none of these replacements may reference a
JDK-version-specific API directly from compiled code. Newer APIs are reached reflectively, gated on
a runtime capability probe (`ThreadAwareSecurityManager.isSecurityManagerSupported()`), so the same
binary selects the legacy path where a `SecurityManager` is installable and the replacement path
where it is not.

## 1. UDF sandbox

User-defined functions previously relied on a runtime `SecurityManager`
(`ThreadAwareSecurityManager` plus a no-permission `ProtectionDomain` on the UDF class loader) to
deny dangerous operations (file/network I/O, process execution, `System.exit`, thread manipulation,
reflection escapes, arbitrary system-property access) while UDF code ran. That runtime check cannot
exist on JDKs where the `SecurityManager` is disabled. It is replaced by a `SecurityManager`-free
sandbox built from a `CREATE FUNCTION`-time allowlist/blocklist bytecode verifier and a filtering
UDF class loader, so dangerous code can never be referenced, linked, or loaded in the first place,
with the existing execution watchdog retained. The legacy `SecurityManager` path is kept where it is
installable, and the new sandbox can be forced on any JDK so it is exercised on all of 11/17/21/25.
For the full design, threat model, and the per-API allowlist/blocklist, see the companion
[`cql3/functions/UDFSecurity.md`](../cql3/functions/UDFSecurity.md).

## 2. JMX authorization Subject resolution

JMX authorization and audit logging need the authenticated `Subject` for the in-flight MBean call.
Historically the JMX layer associated that `Subject` with the current `AccessControlContext` and
Cassandra recovered it with `Subject.getSubject(AccessController.getContext())` (and ran work under
it with `Subject.doAs`). On JDK 24+ `Subject.getSubject(AccessControlContext)` unconditionally
throws `UnsupportedOperationException`; the supported replacement is `Subject.current()` (added in
JDK 18), paired on the calling side with `Subject.callAs`.

`org.apache.cassandra.security.JMXSubjects` provides a single `current()` accessor used by
`AuthorizationProxy` and `AuditLogManager`. It selects the API at runtime: when a `SecurityManager`
is installable it calls `Subject.getSubject(AccessController.getContext())`; otherwise it invokes
`Subject.current()`. Because `Subject.current()` does not exist on JDK 11, it is looked up and
invoked reflectively (resolved once and cached), keeping the class compilable under `source 11`. The
guarantee is unchanged: callers receive exactly the `Subject` bound to the current JMX invocation,
or `null` when there is none.

## 3. Overwriting final fields via Unsafe

Several testing/utility helpers overwrite `final` fields — for example to swap a message
serializer/handler/codec or reset a global clock for a simulation. The old technique cleared the
`FINAL` bit in `Field.modifiers` via reflection and then called `Field.set`. On JDK 22+ this no
longer works: reflection now writes through method handles that honor field finality, and the
`-Djdk.reflect.useDirectMethodHandle=false` flag that restored the old native accessors was removed
(JDK-8309635), so the write throws or silently has no effect.

`ReflectionUtils.writeField(instance, field, value)` replaces the modifiers hack by writing through
`sun.misc.Unsafe` — `staticFieldBase`/`staticFieldOffset` for static fields, `objectFieldOffset` for
instance fields, then the type-appropriate `put*` — which bypasses the `final` check on every
supported JDK (Cassandra already depends on `sun.misc.Unsafe`). All former modifiers-hack call sites
(e.g. `FieldUtil.setInstanceUnsafe`) now route through it. The one inherent limitation is unchanged:
a `static final` compile-time constant the compiler has already inlined cannot be altered, but every
affected field is a reference/object field rather than such a constant.

## 4. Trapping System.exit in tests via Byteman

Tests that drive tools and clusters must catch an attempted `System.exit` instead of letting it kill
the test JVM. This was done with a test-only `SecurityManager` whose `checkExit` threw a catchable
`SystemExitException`. That manager cannot be installed on JDK 24+.

The replacement is a Byteman rule, installed lazily on first use, on `java.lang.Runtime.exit` and
`java.lang.Runtime.halt`: at method entry, when exit blocking is active, the rule throws the
application-loaded `SystemExitException`, giving the same catchable outcome the `checkExit` hook did.
Byteman is used rather than a plain bytecode-rewriting agent because its rule action runs in the
application class loader, so it throws the same `SystemExitException` class the test code catches
(an agent that injected a bootstrap copy would yield a different `Class` that the `catch` would
miss). `SystemExitManager` exposes reference-counted, JVM-global `blockExit()`/`unblockExit()` scopes
that mirror the previous always-installed-while-active manager; both it and the class are `@Shared`
so in-JVM dtests see one counter across instance class loaders.

A single Byteman agent per JVM is owned by `BytemanAgentSupport`, shared by `SystemExitManager`, the
`Injections` framework, and any future caller, so they agree on one `(host, port)` rather than each
attaching its own. Two details are required for JDK 24+: the agent attaches with
`org.jboss.byteman.transform.all=true` (and `addToBoot`) so the bootstrap class `java.lang.Runtime`
can be transformed, and `setPolicy` is always `false` because Byteman's `installPolicy()` calls
`Policy.setPolicy()`, which throws `UnsupportedOperationException` on JDK 24+. Unlike the UDF and JMX
replacements, the test exit interception does not keep a legacy `SecurityManager`/`checkExit` path: the
Byteman rule is used uniformly on every supported JDK, so there is nothing to select between.
