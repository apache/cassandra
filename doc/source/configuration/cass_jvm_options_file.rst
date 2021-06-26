.. _cassandra-jvm-options:

jvm-* files 
===========

Several files for JVM configuration are included in Cassandra. The ``jvm-server.options`` file, and corresponding files ``jvm8-server.options`` and ``jvm11-server.options`` are the main file for settings that affect the operation of the Cassandra JVM on cluster nodes. The file includes startup parameters, general JVM settings such as garbage collection, and heap settings. The ``jvm-clients.options`` and corresponding ``jvm8-clients.options`` and ``jvm11-clients.options`` files can be used to configure JVM settings for clients like ``nodetool`` and the ``sstable`` tools. 

See each file for examples of settings.

.. note:: The ``jvm-*`` files replace the :ref:`cassandra-envsh` file used in Cassandra versions prior to Cassandra 3.0. The ``cassandra-env.sh`` bash script file is still useful if JVM settings must be dynamically calculated based on system settings. The ``jvm-*`` files only store static JVM settings.
