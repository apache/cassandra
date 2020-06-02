.. _cassandra-jvm-options:

jvm-* files 
================================

Several files for JVM configuration are included in Cassandra. The ``jvm-server.options`` file, and corresponding files ``jvm8-server.options`` and jvm11-server.options`` are the main file for settings that affect the operation of the Cassandra JVM, i.e., the server. The file includes startup parameters, general JVM settings, and heap settings. The ``jvm-clients.options`` and corresponding ``jvm8-clients.options`` and ``jvm11-clients.options`` files can be used to configure JVM settings for clients like ``nodetool`` and the ``sstable`` tools. 

See each file for examples of settings.
