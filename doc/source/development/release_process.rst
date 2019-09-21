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

.. attention::

    WORK IN PROGRESS
     * A number of these steps still have been finalised/tested.
     * The use of people.apache.org needs to be replaced with svnpubsub and dist.apache.org


The steps for Release Managers to create, vote and publish releases for Apache Cassandra.

While a committer can perform the initial steps of creating and calling a vote on a proposed release, only a PMC can complete the process of publishing and announcing the release.


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
Include your public key in::

  https://dist.apache.org/repos/dist/release/cassandra/KEYS


Publish your GPG key in a PGP key server, such as `MIT Keyserver <http://pgp.mit.edu/>`_.


Create Release Artifacts
========================

Any committer can perform the following steps to create and call a vote on a proposed release.

Check that no open jira tickets are urgent and currently being worked on.
Also check with a PMC that there's security vulnerabilities currently being worked on in private.

Perform the Release
-------------------

Run the following commands to generate and upload release artifacts, to a nexus staging repository and distribution location::


    cd ~/git
    git clone https://github.com/apache/cassandra-builds.git
    # Edit the variables at the top of `cassandra-builds/cassandra-release/prepare_release.sh`

    # After cloning cassandra-builds repo, the prepare_release.sh is run from the actual cassandra git checkout, 
    # on the branch/commit that we wish to tag for the tentative release along with version number to tag.
    # For example here <version-branch> might be `3.11` and <version> `3.11.3`
    cd ~/git/cassandra/
    git checkout cassandra-<version-branch>
    ../cassandra-builds/cassandra-release/prepare_release.sh -v <version>

If successful, take note of the email text output which can be used in the next section "Call for a Vote".

The ``prepare_release.sh`` script does not yet generate and upload the rpm distribution packages.
To generate and upload them do::

    cd ~/git/cassandra-build
    docker build -f docker/centos7-image.docker docker/
    docker run --rm -v `pwd`/dist:/dist `docker images -f label=org.cassandra.buildenv=centos -q` /home/build/build-rpms.sh <version>-tentative
    rpmsign --addsign dist/*.rpm

For more information on the above steps see the `cassandra-builds documentation <https://github.com/apache/cassandra-builds>`_.
The next step is to copy and commit these binaries to staging svnpubsub::

    # FIXME the following commands is wrong while people.apache.org is still used instead of svnpubsub and dist.apache.org
    cd ~/git
    svn co https://dist.apache.org/repos/dist/dev/cassandra cassandra-dist-dev
    mkdir cassandra-dist-dev/<version>
    cp cassandra-build/dist/*.rpm cassandra-dist-dev/<version>/

    svn add cassandra-dist-dev/<version>
    svn ci cassandra-dist-dev/<version>

After committing the binaries to staging, increment the version number in Cassandra on the `cassandra-<version-branch>`

    cd ~/git/cassandra/
    git checkout cassandra-<version-branch>
    edit build.xml          # update `<property name="base.version" value="…"/> `
    edit debian/changelog   # add entry for new version
    edit CHANGES.txt        # add entry for new version
    git commit -m "Update version to <next-version>" build.xml debian/changelog CHANGES.txt
    git push

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



Post-vote operations
====================

Any PMC can perform the following steps to formalize and publish a successfully voted release.

Publish Artifacts
-----------------

Run the following commands to publish the voted release artifacts::

    cd ~/git
    git clone https://github.com/apache/cassandra-builds.git
    # edit the variables at the top of `finish_release.sh`

    # After cloning cassandra-builds repo, `finish_release.sh` is run from the actual cassandra git checkout,
    # on the tentative release tag that we wish to tag for the final release version number tag.
    cd ~/git/cassandra/
    git checkout <version>-tentative
    ../cassandra-builds/cassandra-release/finish_release.sh -v <version> <staging_number>

If successful, take note of the email text output which can be used in the next section "Send Release Announcement".
The output will also list the next steps that are required. The first of these is to commit changes made to your https://dist.apache.org/repos/dist/release/cassandra/ checkout.


Promote Nexus Repository
------------------------

 * Login to `Nexus repository <https://repository.apache.org>`_ again.
 * Click on "Staging" and then on the repository with id "cassandra-staging".
 * Find your closed staging repository, right click on it and choose "Promote".
 * Select the "Releases" repository and click "Promote".
 * Next click on "Repositories", select the "Releases" repository and validate that your artifacts exist as you expect them.

Sign and Upload Distribution Packages to Bintray
---------------------------------------

Run the following command::

    cd ~/git
    # FIXME the next command is wrong while people.apache.org is used instead of svnpubsub and dist.apache.org
    svn mv https://dist.apache.org/repos/dist/dev/cassandra/<version> https://dist.apache.org/repos/dist/release/cassandra/

    # Create the yum metadata, sign the metadata, and sign some files within the signed repo metadata that the ASF sig tool errors out on
    svn co https://dist.apache.org/repos/dist/release/cassandra/redhat/ cassandra-dist-redhat
    cd cassandra-dist-redhat/<abbreviated-version>x/
    createrepo .
    gpg --detach-sign --armor repodata/repomd.xml
    for f in `find repodata/ -name *.bz2`; do
      gpg --detach-sign --armor $f;
    done

    svn co https://dist.apache.org/repos/dist/release/cassandra/<version> cassandra-dist-<version>
    cd cassandra-dist-<version>
    cassandra-build/cassandra-release/upload_bintray.sh cassandra-dist-<version>


Update and Publish Website
--------------------------

See `docs https://svn.apache.org/repos/asf/cassandra/site/src/README`_ for building and publishing the website.
Also update the CQL doc if appropriate.

Release version in JIRA
-----------------------

Release the JIRA version.

  * In JIRA go to the version that you want to release and release it.
  * Create a new version, if it has not been done before.

Update to Next Development Version
----------------------------------

Edit and commit ``build.xml`` so the base.version property points to the next version.

Wait for Artifacts to Sync
--------------------------

Wait for the artifacts to sync at http://www.apache.org/dist/cassandra/

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
Also check people.apache.org as previous release scripts used it.
