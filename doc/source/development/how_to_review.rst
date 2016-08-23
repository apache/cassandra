.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Review Checklist
****************

When reviewing tickets in Apache JIRA, the following items should be covered as part of the review process:

**General**

 * Does it conform to the :doc:`code_style` guidelines?
 * Is there any redundant or duplicate code?
 * Is the code as modular as possible?
 * Can any singletons be avoided?
 * Can any of the code be replaced with library functions?
 * Are units of measurement used in the code consistent, both internally and with the rest of the ecosystem?

**Error-Handling**

 * Are all data inputs and outputs checked (for the correct type, length, format, and range) and encoded?
 * Where third-party utilities are used, are returning errors being caught?
 * Are invalid parameter values handled?
 * Are any Throwable/Exceptions passed to the JVMStabilityInspector?
 * Are errors well-documented? Does the error message tell the user how to proceed?
 * Do exceptions propagate to the appropriate level in the code?

**Documentation**

 * Do comments exist and describe the intent of the code (the "why", not the "how")?
 * Are javadocs added where appropriate?
 * Is any unusual behavior or edge-case handling described?
 * Are data structures and units of measurement explained?
 * Is there any incomplete code? If so, should it be removed or flagged with a suitable marker like ‘TODO’?
 * Does the code self-document via clear naming, abstractions, and flow control?
 * Have NEWS.txt, the cql3 docs, and the native protocol spec been updated if needed?
 * Is the ticket tagged with "client-impacting" and "doc-impacting", where appropriate?
 * Has lib/licences been updated for third-party libs? Are they Apache License compatible?
 * Is the Component on the JIRA ticket set appropriately?

**Testing**

 * Is the code testable? i.e. don’t add too many or hide dependencies, unable to initialize objects, test frameworks can use methods etc.
 * Do tests exist and are they comprehensive?
 * Do unit tests actually test that the code is performing the intended functionality?
 * Could any test code use common functionality (e.g. ccm, dtest, or CqlTester methods) or abstract it there for reuse?
 * If the code may be affected by multi-node clusters, are there dtests?
 * If the code may take a long time to test properly, are there CVH tests?
 * Is the test passing on CI for all affected branches (up to trunk, if applicable)? Are there any regressions?
 * If patch affects read/write path, did we test for performance regressions w/multiple workloads?
 * If adding a new feature, were tests added and performed confirming it meets the expected SLA/use-case requirements for the feature?

**Logging**

 * Are logging statements logged at the correct level?
 * Are there logs in the critical path that could affect performance?
 * Is there any log that could be added to communicate status or troubleshoot potential problems in this feature?
 * Can any unnecessary logging statement be removed?

