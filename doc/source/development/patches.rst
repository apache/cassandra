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

.. highlight:: none

Contributing Code Changes
*************************

Choosing What to Work on
========================

Submitted patches can include bug fixes, changes to the Java code base, improvements for tooling (both Java or Python), documentation, testing or any other changes that requires changing the code base. Although the process of contributing code is always the same, the amount of work and time it takes to get a patch accepted also depends on the kind of issue you're addressing.

As a general rule of thumb:
 * Major new features and significant changes to the code based will likely not going to be accepted without deeper discussion within the `developer community <http://cassandra.apache.org/community/>`_
 * Bug fixes take higher priority compared to features
 * The extend to which tests are required depend on how likely your changes will effect the stability of Cassandra in production. Tooling changes requires fewer tests than storage engine changes.
 * Less complex patches will be faster to review: consider breaking up an issue into individual tasks and contributions that can be reviewed separately

.. hint::

   Not sure what to work? Just pick an issue tagged with the `low hanging fruit label <https://issues.apache.org/jira/secure/IssueNavigator.jspa?reset=true&jqlQuery=project+=+12310865+AND+labels+=+lhf+AND+status+!=+resolved>`_ in JIRA, which we use to flag issues that could turn out to be good starter tasks for beginners.

Before You Start Coding
=======================

Although contributions are highly appreciated, we do not guarantee that each contribution will become a part of Cassandra. Therefor it's generally a good idea to first get some feedback on the things you plan to work on, especially about any new features or major changes to the code base. You can reach out to other developers on the mailing list or IRC channel listed on our `community page <http://cassandra.apache.org/community/>`_.

You should also
 * Avoid redundant work by searching for already reported issues in `JIRA <https://issues.apache.org/jira/browse/CASSANDRA>`_
 * Create a new issue early in the process describing what you're working on - not just after finishing your patch
 * Link related JIRA issues with your own ticket to provide a better context
 * Update your ticket from time to time by giving feedback on your progress and link a GitHub WIP branch with your current code
 * Ping people who you actively like to ask for advice on JIRA by `mentioning users <https://confluence.atlassian.com/conf54/confluence-user-s-guide/sharing-content/using-mentions>`_

There are also some fixed rules that you need to be aware:
 * Patches will only be applied to branches by following the release model
 * Code must be testable
 * Code must follow the :doc:`code_style` convention
 * Changes must not break compatibility between different Cassandra versions
 * Contributions must be covered by the Apache License

Choosing the Right Branches to Work on
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are currently multiple Cassandra versions maintained in individual branches:

======= ======
Version Policy
======= ======
3.x     Tick-tock (see below)
3.0     Bug fixes only
2.2     Bug fixes only
2.1     Critical bug fixes only
======= ======

Corresponding branches in git are easy to recognize as they are named ``cassandra-<release>`` (e.g. ``cassandra-3.0``). The ``trunk`` branch is an exception, as it contains the most recent commits from all other branches and is used for creating new branches for future tick-tock releases.

Tick-Tock Releases
""""""""""""""""""

New releases created as part of the `tick-tock release process <http://www.planetcassandra.org/blog/cassandra-2-2-3-0-and-beyond/>`_ will either focus on stability (odd version numbers) or introduce new features (even version numbers). Any code for new Cassandra features you should be based on the latest, unreleased 3.x branch with even version number or based on trunk.

Bug Fixes
"""""""""

Creating patches for bug fixes is a bit more complicated as this will depend on how many different versions of Cassandra are affected. In each case, the order for merging such changes will be ``cassandra-2.1`` -> ``cassandra-2.2`` -> ``cassandra-3.0`` -> ``cassandra-3.x`` -> ``trunk``. But don't worry, merging from 2.1 would be the worst case for bugs that affect all currently supported versions, which isn't very common. As a contributor, you're also not expected to provide a single patch for each version. What you need to do however is:

 * Be clear about which versions you could verify to be affected by the bug
 * For 2.x: ask if a bug qualifies to be fixed in this release line, as this may be handled on case by case bases
 * If possible, create a patch against the lowest version in the branches listed above (e.g. if you found the bug in 3.9 you should try to fix it already in 3.0)
 * Test if the patch can be merged cleanly across branches in the direction listed above
 * Be clear which branches may need attention by the committer or even create custom patches for those if you can

Creating a Patch
================

So you've finished coding and the great moment arrives: it's time to submit your patch!

 1. Create a branch for your changes if you haven't done already. Many contributors name their branches based on ticket number and Cassandra version, e.g. ``git checkout -b 12345-3.0``
 2. Verify that you follow Cassandra's :doc:`code_style`
 3. Make sure all tests (including yours) pass using ant as described in :doc:`testing`. If you suspect a test failure is unrelated to your change, it may be useful to check the test's status by searching the issue tracker or looking at `CI <https://cassci.datastax.com/>`_ results for the relevant upstream version.  Note that the full test suites take many hours to complete, so it is common to only run specific relevant tests locally before uploading a patch.  Once a patch has been uploaded, the reviewer or committer can help setup CI jobs to run the full test suites.
 4. Consider going through the :doc:`how_to_review` for your code. This will help you to understand how others will consider your change for inclusion.
 5. Don’t make the committer squash commits for you in the root branch either. Multiple commits are fine - and often preferable - during review stage, especially for incremental review, but once +1d, do either:

   a. Attach a patch to JIRA with a single squashed commit in it (per branch), or
   b. Squash the commits in-place in your branches into one

 6. Include a CHANGES.txt entry (put it at the top of the list), and format the commit message appropriately in your patch ending with the following statement on the last line: ``patch by X; reviewed by Y for CASSANDRA-ZZZZZ``
 7. When you're happy with the result, create a patch:

   ::

      git add <any new or modified file>
      git commit -m '<message>'
      git format-patch HEAD~1
      mv <patch-file> <ticket-branchname.txt> (e.g. 12345-trunk.txt, 12345-3.0.txt)

   Alternatively, many contributors prefer to make their branch available on GitHub. In this case, fork the Cassandra repository on GitHub and push your branch:

   ::

      git push --set-upstream origin 12345-3.0

 8. To make life easier for your reviewer/committer, you may want to make sure your patch applies cleanly to later branches and create additional patches/branches for later Cassandra versions to which your original patch does not apply cleanly. That said, this is not critical, and you will receive feedback on your patch regardless.
 9. Attach the newly generated patch to the ticket/add a link to your branch and click "Submit Patch" at the top of the ticket. This will move the ticket into "Patch Available" status, indicating that your submission is ready for review.
 10. Wait for other developers or committers to review it and hopefully +1 the ticket (see :doc:`how_to_review`). If your change does not receive a +1, do not be discouraged. If possible, the reviewer will give suggestions to improve your patch or explain why it is not suitable.
 11. If the reviewer has given feedback to improve the patch, make the necessary changes and move the ticket into "Patch Available" once again.

Once the review process is complete, you will receive a +1. Wait for a committer to commit it. Do not delete your branches immediately after they’ve been committed - keep them on GitHub for a while. Alternatively, attach a patch to JIRA for historical record. It’s not that uncommon for a committer to mess up a merge. In case of that happening, access to the original code is required, or else you’ll have to redo some of the work.


