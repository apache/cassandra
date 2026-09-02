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

# User-Defined Function (UDF) Security Sandbox

## Overview

A user-defined function (UDF) lets an operator extend CQL with custom server-side logic written
in Java. The function body supplied in a `CREATE FUNCTION` statement is compiled on the fly into a
JVM class that runs inside the Cassandra process, in the same address space as the storage engine,
on the same heap, and (potentially) on the request-handling path of every read that references it.

That power makes UDFs inherently dangerous. A UDF body is ordinary Java: left unconstrained it could
call `System.exit()` and take down the node, spawn external processes, open files and sockets, read
or write arbitrary system properties, reach into the running server through reflection or method
handles, manipulate threads, or pull in JVM-internal/unsafe APIs. Because the author of a UDF is not
necessarily the operator who runs the cluster, and because UDFs are replicated through schema to every
node, untrusted Java code must be executed under a strict sandbox.

The threat model the sandbox must defend against is, concretely, that UDF code must **not** be able to:

- terminate or destabilize the JVM (`System.exit`, `Runtime.exit`, `Runtime.halt`, `Shutdown.exit`);
- execute external processes (`Runtime.exec`, `ProcessBuilder`, `ProcessImpl`, `ProcessHandle`);
- perform arbitrary file, network, or socket I/O;
- escape the sandbox via reflection, method handles, var handles, the class-file API, or the
  Foreign Function & Memory API;
- create or manipulate threads, thread groups, or class loaders to reach unguarded code;
- read or write sensitive system properties, environment variables, or load native libraries;
- reach JVM-internal or `sun.*`/`jdk.internal.*`/unsafe APIs.

Cassandra enforces this with two interchangeable mechanisms that present the same security guarantee.
Which one is active depends on the JDK the node is running on and on an explicit operator override.

## Why two mechanisms

Historically the sandbox was built around a `java.lang.SecurityManager`: UDF code ran on a dedicated,
"secured" thread, and a custom `SecurityManager` denied dangerous permissions whenever the current
thread was one of those.

The `SecurityManager` API is being permanently retired from the JVM. Starting with JDK 24,
`System.setSecurityManager(non-null)` unconditionally throws `UnsupportedOperationException`, and
`Policy.setPolicy` likewise throws — there is simply no way to install a `SecurityManager` on those
runtimes. The legacy runtime sandbox therefore cannot exist on current and future JDKs.

To keep UDFs safe without a `SecurityManager`, Cassandra also implements a **SecurityManager-free
sandbox** that achieves the same guarantee through static analysis and class-loader confinement rather
than runtime permission checks. The same property file lets operators choose between the two, so the
SecurityManager-free path can also be exercised on older JDKs (11/17/21) for testing.

The key insight that makes the SecurityManager-free path viable is that the backbone of the sandbox —
a filtering UDF class loader — never depended on the `SecurityManager` in the first place. It restricts
which classes a UDF may even link or load, and it works on every JDK. The `SecurityManager` only ever
added a thin runtime layer on top of it (principally policing the handful of dangerous `java.lang.System`
methods that the class loader has to leave reachable). The SecurityManager-free sandbox closes that
remaining gap statically, at `CREATE FUNCTION` time.

## How UDFs are compiled and confined

Regardless of which mechanism is selected, every Java UDF goes through the same pipeline in
`JavaBasedUDFunction`:

1. The function body is substituted into a fixed source template that declares a `public final` class
   `extends JavaUDF` with a single generated execute method. Each UDF is emitted into its own package
   so that one UDF cannot reference another's generated class.
2. The source is compiled in-process by the Eclipse compiler (ECJ) against a restricted name
   environment: type resolution goes through `UDFunction`'s filtering class loader, so a UDF that
   references a forbidden type fails to compile (`type cannot be resolved`) rather than at call time.
3. The emitted bytecode is run through a byte-code verifier (`UDFByteCodeVerifier`) before the class is
   ever loaded. A UDF that uses a forbidden construct or calls a forbidden method is rejected, and the
   `CREATE FUNCTION` statement fails with an `InvalidRequestException`.
4. Only after verification passes is the class defined by `EcjTargetClassLoader` (a child of the
   filtering UDF class loader) and instantiated.

This ordering matters: **verification happens at function-creation time wherever possible**, so a
malicious or malformed UDF is rejected when it is created, not merely when it is first invoked. The
generated class is structurally constrained too — it must be `public final`, extend `JavaUDF`, declare
no fields, no static initializer, and no extra methods or inner classes; the verifier rejects anything
else, and a compilation that emits more than one class file (an inner/nested/anonymous class) is
refused because the verifier inspects only a single class.

### The filtering UDF class loader (shared by both mechanisms)

`UDFunction.UDFClassLoader` is the foundation of the sandbox on all JDKs. It overrides `loadClass`,
`findClass`, and the resource-lookup methods so that a class or resource is resolvable only if it
passes `secureResource()`, which applies an **allowlist** first and then a **denylist**:

- The request must match one of the `allowedPatterns` prefixes — the small set of types a legitimate
  UDF needs: `java.lang.*` numerics/`String`/`Math`/`StringBuilder`, `java.math`, `java.text`,
  `java.time`, `java.util` collections, `java.nio.ByteBuffer`/`Buffer`, the `InetAddress` family, a
  handful of Cassandra UDF-API types (`JavaUDF`, `UDFContext`, `Arguments`, `UDFDataType`, the
  driver-derived `types` package, `ProtocolVersion`), and `IOException`/`Serializable`. Anything not on
  the allowlist is simply not found — `ClassNotFoundException` at runtime, unresolvable at compile time.
- A matching class is then checked against `disallowedPatterns`, which carves dangerous classes back
  out of the otherwise-allowed prefixes (mostly out of `java/lang/`). This blocks, among others:
  `Runtime`, `Process`/`ProcessBuilder`/`ProcessImpl`/`ProcessEnvironment`, `Shutdown`,
  `Thread`/`ThreadGroup`/`ThreadLocal`/`InheritableThreadLocal`, `Package`, `Compiler`,
  `java.lang.reflect.*`, `java.lang.invoke.*` (including `MethodHandles`/`VarHandle`),
  `java.lang.instrument.*`, `java.lang.management.*`, `java.lang.ref.*`, and the post-JDK-8 additions
  `ProcessHandle`, `StackWalker`, `java.lang.foreign.*` (Foreign Function & Memory API, JDK 22),
  and `java.lang.classfile.*` (Class-File API, JDK 24). It also blocks `java.util.concurrent.*`,
  `java.util.function.*`, `java.util.stream.*`, `ServiceLoader`, `Timer`, and the
  `logging`/`prefs`/`jar`/`zip`/`spi` utility packages.

The net effect is that a UDF can never link or load a file/network/process/thread/reflection class:
the bytes for those classes are unreachable through the only class loader the UDF is allowed to use.
This holds with or without a `SecurityManager`, which is why it is the common base for both mechanisms.

> `java.lang.System` is a deliberate exception. UDFs legitimately need `System.nanoTime`,
> `System.currentTimeMillis`, and `System.arraycopy`, so `java/lang/System` must remain loadable. Its
> dangerous *methods* are policed separately (see below). `java.lang.Module` / `java.lang.ModuleLayer`
> are another exception: they cannot be denied at the class-loader level because `java.lang.Class`
> (which every UDF needs) references `Module` in its signature, so denying `Module` would make `Class`
> itself unresolvable. They are blocked at the byte-code verifier instead.

The generated UDF class is defined with an empty `PermissionCollection` (`noPermissions`) and an
empty `ProtectionDomain`. Under the legacy mechanism this is the policy the `SecurityManager` consults;
under the SecurityManager-free mechanism it is inert but harmless, since the class can reference nothing
dangerous in the first place.

## Mechanism A — legacy thread-based SecurityManager

Used when a `SecurityManager` can be installed (JDK < 24) and the selected mechanism resolves to the
SecurityManager path.

UDF execution is dispatched onto a dedicated executor whose threads belong to a `SecurityThreadGroup`.
`ThreadAwareSecurityManager` (a custom `SecurityManager` paired with a custom `Policy`) performs access
checks **only** when the current thread is one of those secured threads, which keeps the cost off the
normal request path. On a secured thread it denies:

- `modifyThread` and `modifyThreadGroup`, so UDF code cannot touch the watchdog or other threads;
- `setSecurityManager` (so a UDF cannot uninstall the sandbox), subject to the insecure opt-in below;
- every permission the empty UDF policy does not grant — file I/O, sockets, process execution,
  `System.exit`, system-property writes, class-loader creation, and most reflection. A small set of
  permissions is granted as exceptions because the bundled driver/codec code needs them
  (`accessDeclaredMembers`, `suppressAccessChecks`, `getClassLoader`, and a couple of Nashorn/Dynalink
  permissions).

Package access is additionally gated by `SecurityThreadGroup.isPackageAllowed`. The real teeth, though,
are the empty `ProtectionDomain` on the UDF class loader plus these `checkPermission` overrides; the
class loader has already removed the dangerous classes, and the `SecurityManager` covers what remains
reachable at runtime (chiefly the `java.lang.System` methods).

Because the legacy mechanism polices `System.*` at runtime, the byte-code verifier used on this path is
the *base* verifier, which does not statically block the `System` methods.

## Mechanism B — the SecurityManager-free sandbox

Used by default on JDK 24+ (where no `SecurityManager` can be installed), and on any JDK when explicitly
selected. It is the filtering class loader described above, hardened to full parity by closing the one
gap the `SecurityManager` used to cover, without any runtime permission checks.

### Static blocking of dangerous `java.lang.System` methods (creation time)

A second verifier instance, the *sandbox* verifier, carries everything the base verifier blocks **plus**
a per-method denylist for `java/lang/System`. Because `java.lang.System` stays loadable (UDFs need its
safe timing/array methods), the sandbox rejects the dangerous ones at `CREATE FUNCTION` time instead of
at runtime:

`exit`, `setSecurityManager`, `getSecurityManager`, `setProperty`, `getProperty`, `getProperties`,
`setProperties`, `clearProperty`, `getenv`, `load`, `loadLibrary`, `setIn`, `setOut`, `setErr`,
`inheritedChannel`, `console`, and `getLogger`.

It additionally blocks the **property-read aliases** that would otherwise leak system properties past
the `System.getProperty` block, since they are thin wrappers over it:

- `Integer.getInteger`
- `Long.getLong`
- `Boolean.getBoolean`

and the system-logger back door `System$LoggerFinder.getLoggerFinder` (which the empty UDF policy denied
under the `SecurityManager` via `RuntimePermission("loggerFinder")`), so the SecurityManager-free path
does not expose a capability the legacy path withheld.

This static blocking is airtight precisely because the class loader already makes the reflective routes
to these methods impossible: `java.lang.reflect.*` and `java.lang.invoke.*` are not loadable, so a UDF
cannot reach `System.exit` (etc.) indirectly. There is nothing left for a runtime check to catch.

### Base verifier rules (both mechanisms)

Both verifiers always reject a common set of escape routes regardless of mechanism or flags:

- `Class.forName`, `Class.getClassLoader`, `Class.getResource(AsStream)`, and `Class.getModule`
  (the entry point to the JDK module API);
- the classes `java.lang.Module` and `java.lang.ModuleLayer` (blocked here rather than at the class
  loader, for the `Class.getModule` signature reason noted above);
- the dangerous `java.lang.ClassLoader` methods (`loadClass`, `getResource*`, `getSystem*`, assertion
  setters);
- `ByteBuffer.allocateDirect` (off-heap allocation);
- the `InetAddress`/`Inet4Address`/`Inet6Address` lookup methods that perform DNS or ICMP traffic
  (`getByName`, `getAllByName`, `getByAddress`, `getLocalHost`, `getHostName`,
  `getCanonicalHostName`, `isReachable`), and the classes `NetworkInterface` and `SocketException`.

### Runtime guards (both mechanisms)

Static verification bounds *what code* a UDF may contain; it does not bound *how long* a well-formed
UDF runs. That is handled by a runtime watchdog that is independent of the security mechanism and
applies on both paths.

When threaded UDF execution is enabled, each invocation is submitted to the UDF executor and awaited
with a timeout (`executeAsync`/`async` in `UDFunction`). On exceeding the configured *warn* timeout a
client warning is emitted; on exceeding the *fail* timeout the invocation is abandoned, and — depending
on the configured timeout policy — the node may be stopped to avoid a wedged UDF holding a thread
indefinitely. The watchdog runs UDFs on the secured `SecurityThreadGroup` executor and is what bounds
runaway loops, excessive allocation, and accidental blocking. It is intentionally orthogonal to the
sandbox: it constrains resource consumption, while verification and the class loader constrain
capability.

## Selecting the mechanism

The mechanism is chosen by the system property `cassandra.udf.security_mechanism`
(`CassandraRelevantProperties.UDF_SECURITY_MECHANISM`), which accepts:

| Value             | Behavior |
|-------------------|----------|
| `auto` (default)  | Use the legacy `SecurityManager` mechanism when a `SecurityManager` can be installed (JDK < 24); otherwise use the SecurityManager-free sandbox. |
| `securitymanager` | Force the legacy mechanism. Fails fast at startup on JDK 24+, where a `SecurityManager` cannot be installed, rather than silently running UDFs unsandboxed. |
| `sandbox`         | Force the SecurityManager-free sandbox on any JDK. Primarily used to exercise the new mechanism on JDK 11/17/21. |

`auto` resolves at startup via `ThreadAwareSecurityManager.isSecurityManagerSupported()`, which reports
`true` only on `Runtime.version().feature() < 24`. When the SecurityManager-free path is in effect
nothing is installed at startup; the class loader and creation-time verification carry the sandbox. An
invalid value raises a `ConfigurationException`.

`JavaBasedUDFunction.verifierFor()` then selects the verifier *per `CREATE FUNCTION`* (not once at
class-init), so a change to the relevant flags takes effect without a JVM restart:

- When a `SecurityManager` is in use, the **base** verifier is always selected — `System.*` is policed by
  the `SecurityManager` at runtime (or, in the legacy synchronous path, by the class loader's
  `disallowedPatternsSyncUDF`, which denies `java.lang.System` outright).
- When the SecurityManager-free sandbox is in use, the **sandbox** verifier (with the `System.*` block)
  is selected, except for the explicit insecure opt-in described next.

## Interaction with `allow_extra_insecure_udfs`

`allow_extra_insecure_udfs` (cassandra.yaml; `DatabaseDescriptor.allowExtraInsecureUDFs()`, default
`false`) is an explicit, discouraged escape hatch that deliberately permits UDFs to call dangerous
`java.lang.System` methods. Enabling it logs a warning that allowing `java.lang.System.*` access in UDFs
is dangerous and not recommended.

It is only meaningful together with the *synchronous* (non-threaded) UDF execution mode. The legacy
behavior was that the dangerous `System.*` calls were blocked **unless** UDF threads were disabled *and*
`allow_extra_insecure_udfs` was `true` — in which case UDFs ran on a non-secured thread where `System.*`
was permitted.

The SecurityManager-free sandbox preserves exactly that policy statically. `verifierFor()` selects the
`System.*`-blocking sandbox verifier whenever
`enableUserDefinedFunctionsThreads() || !allowExtraInsecureUDFs()`, and falls back to the base verifier
(no `System.*` block) only for the equivalent insecure opt-in (threads disabled *and*
`allow_extra_insecure_udfs=true`). The escape hatch is thus neither tightened nor loosened by the move
away from the `SecurityManager`; it behaves identically on both paths.

## Summary

- The filtering UDF class loader is the common backbone on every JDK: an allowlist of safe types plus a
  denylist that removes dangerous classes, so a UDF can never link or load file/network/process/thread/
  reflection APIs.
- On JDK < 24 the legacy thread-based `SecurityManager` adds a runtime layer that polices the few
  dangerous `java.lang.System` methods left reachable, plus thread/thread-group/security-manager access.
- On JDK 24+ (and whenever forced) the SecurityManager-free sandbox replaces that runtime layer with
  static rejection of the same dangerous `System.*` methods (and their property-read aliases and the
  logger back door) at `CREATE FUNCTION` time, which is complete because reflective routes to those
  methods are already unreachable.
- Verification runs at function-creation time, so unsafe UDFs are rejected at `CREATE FUNCTION` rather
  than at call time, and a mechanism-independent watchdog bounds UDF run time at execution.
- `cassandra.udf.security_mechanism` (`auto`/`securitymanager`/`sandbox`) selects the mechanism, and
  `allow_extra_insecure_udfs` provides the same explicit, discouraged `System.*` opt-in on both paths.
