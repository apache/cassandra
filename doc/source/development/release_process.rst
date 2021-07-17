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
..  release_process:

Release Process
***************

.. contents:: :depth: 3

| 
|



The steps for Release Managers to create, vote and publish releases for Apache Cassandra.

While a committer can perform the initial steps of creating and calling a vote on a proposed release, only a PMC member can complete the process of publishing and announcing the release.


Prerequisites
=============

Background docs
 * `ASF Release Policy <http://www.apache.org/legal/release-policy.html>`_
 * `ASF Release Distribution Policy <http://www.apache.org/dev/release-distribution>`_
 * `ASF Release Best Practices <http://www.eu.apache.org/dev/release-publishing.html>`_


A debian based linux OS is required to run the release steps from. Debian-based distros provide the required RPM, dpkg and repository management tools.


Create and publish your GPG key
-------------------------------

To create a GPG key, follow the `guidelines <http://www.apache.org/dev/openpgp.html>`_.
The key must be 4096 bit RSA.
Include your public key in::

  https://dist.apache.org/repos/dist/release/cassandra/KEYS


Publish your GPG key in a PGP key server, such as `MIT Keyserver <http://pgp.mit.edu/>`_.

Artifactory account with access to Apache organisation
------------------------------------------------------

Publishing a successfully voted upon release requires Artifactory access using your Apache LDAP credentials. Please verify that you have logged into Artifactory `here <https://apache.jfrog.io/>`_.


Create Release Artifacts
========================

Any committer can perform the following steps to create and call a vote on a proposed release.

Check that there are no open urgent jira tickets currently being worked on. Also check with the PMC that there's security vulnerabilities currently being worked on in private.'
Current project habit is to check the timing for a new release on the dev mailing lists.

Perform the Release
-------------------

Run the following commands to generate and upload release artifacts, to the ASF nexus staging repository and dev distribution location::


    cd ~/git
    git clone https://github.com/apache/cassandra-builds.git
    git clone https://github.com/apache/cassandra.git

    # Edit the variables at the top of the `prepare_release.sh` file
    edit cassandra-builds/cassandra-release/prepare_release.sh

    # Ensure your 4096 RSA key is the default secret key
    edit ~/.gnupg/gpg.conf # update the `default-key` line
    edit ~/.rpmmacros # update the `%gpg_name <key_id>` line

    # Ensure DEBFULLNAME and DEBEMAIL is defined and exported, in the debian scripts configuration
    edit ~/.devscripts

    # The prepare_release.sh is run from the actual cassandra git checkout,
    # on the branch/commit that we wish to tag for the tentative release along with version number to tag.
    cd cassandra
    git switch cassandra-<version-branch>

    # The following cuts the release artifacts (including deb and rpm packages) and deploy to staging environments
    ../cassandra-builds/cassandra-release/prepare_release.sh -v <version>

Follow the prompts.

If building the deb or rpm packages fail, those steps can be repeated individually using the `-d` and `-r` flags, respectively.

Call for a Vote
===============

Fill out the following email template and send to the dev mailing list::

    I propose the following artifacts for release as <version>.

    sha1: <git-sha>

    Git: https://gitbox.apache.org/repos/asf?p=cassandra.git;a=shortlog;h=refs/tags/<version>-tentative

    Artifacts: https://repository.apache.org/content/repositories/orgapachecassandra-<nexus-id>/org/apache/cassandra/apache-cassandra/<version>/

    Staging repository: https://repository.apache.org/content/repositories/orgapachecassandra-<nexus-id>/

    The distribution packages are available here: https://dist.apache.org/repos/dist/dev/cassandra/${version}/

    The vote will be open for 72 hours (longer if needed).

    [1]: (CHANGES.txt) https://git1-us-west.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=CHANGES.txt;hb=<version>-tentative
    [2]: (NEWS.txt) https://git1-us-west.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=NEWS.txt;hb=<version>-tentative


Post Failed Vote Operations
===========================

Delete Artifacts
-----------------

Delete the staged artifacts

	svn rm -m "cassandra-<version> vote failed, ref: <link_to_vote_result>" https://dist.apache.org/repos/dist/dev/cassandra/<version>


Drop Nexus Repository
------------------------

* Login to `Nexus repository <https://repository.apache.org>`_ again.
* Click on "Staging Repositories".
* Find your closed staging repository, select it and then click "Drop".


Remove Git Tag
--------------

Remove the <version>-tentative tag, locally and remote

	git tag -d 4.0.0-tentative
	git push --delete origin 4.0.0-tentative


Post Successful Vote Operations
===============================

Any PMC member can perform the following steps to formalize and publish a successfully voted release.

Publish Artifacts
-----------------

Run the following commands to publish the voted release artifacts::

    cd ~/git
    # edit the variables at the top of the `finish_release.sh` file
    edit cassandra-builds/cassandra-release/finish_release.sh

    # After cloning cassandra-builds repo, `finish_release.sh` is run from the actual cassandra git checkout,
    # on the tentative release tag that we wish to tag for the final release version number tag.
    cd ~/git/cassandra/
    git checkout <version>-tentative
    ../cassandra-builds/cassandra-release/finish_release.sh -v <version>

If successful, take note of the email text output which can be used in the next section "Send Release Announcement".
The output will also list the next steps that are required.


Release Nexus Repository
------------------------

* Login to `Nexus repository <https://repository.apache.org>`_ again.
* Click on "Staging Repositories".
* Find your closed staging repository, right click on it and choose "Release".
* Next click on "Repositories", select the "Releases" repository and validate that your artifacts exist as you expect them.


Update and Publish Website
--------------------------

See `docs <https://svn.apache.org/repos/asf/cassandra/site/src/README>`_ for building and publishing the website.

Also update the CQL doc if appropriate.

Release version in JIRA
-----------------------

Release the JIRA version.

* In JIRA go to the version that you want to release and release it.
* Create a new version, if it has not been done before.

Update to Next Development Version
----------------------------------

Update the codebase to point to the next development version::

    cd ~/git/cassandra/
    git checkout cassandra-<version-branch>
    edit build.xml          # update `<property name="base.version" value="…"/> `
    edit debian/changelog   # add entry for new version
    edit CHANGES.txt        # add entry for new version, move up any entries that were added after the release was cut and staged
    git commit -m "Increment version to <next-version>" build.xml debian/changelog CHANGES.txt

    # …and forward merge and push per normal procedure


Wait for Artifacts to Sync
--------------------------

Wait for the artifacts to sync at https://downloads.apache.org/cassandra/

Send Release Announcement
-------------------------

Fill out the following email template and send to both user and dev mailing lists::

    The Cassandra team is pleased to announce the release of Apache Cassandra version <version>.

    Apache Cassandra is a fully distributed database. It is the right choice
    when you need scalability and high availability without compromising
    performance.

     http://cassandra.apache.org/

    Downloads of source and binary distributions are listed in our download
    section:

     http://cassandra.apache.org/download/

    This version is <the first|a bug fix> release[1] on the <version-base> series. As always,
    please pay attention to the release notes[2] and let us know[3] if you
    were to encounter any problem.

    Enjoy!

    [1]: (CHANGES.txt) https://git1-us-west.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=CHANGES.txt;hb=<version>
    [2]: (NEWS.txt) https://git1-us-west.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=NEWS.txt;hb=<version>
    [3]: https://issues.apache.org/jira/browse/CASSANDRA

Update Slack Cassandra topic
---------------------------

Update topic in ``cassandra`` :ref:`Slack room <slack>`
    /topic cassandra.apache.org | Latest releases: 3.11.4, 3.0.18, 2.2.14, 2.1.21 | ask, don't ask to ask

Tweet from @Cassandra
---------------------

Tweet the new release, from the @Cassandra account

Delete Old Releases
-------------------

As described in `When to Archive <http://www.apache.org/dev/release.html#when-to-archive>`_.

An example of removing old releases::

    svn rm https://dist.apache.org/repos/dist/release/cassandra/<previous_version>