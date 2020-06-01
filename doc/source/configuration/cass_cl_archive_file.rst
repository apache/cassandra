.. _cassandra-cl-archive:

commitlog-archiving.properties file 
================================

The ``commitlog-archiving.properties`` configuration file can optionally set commands that are executed when archiving or restoring a commitlog segment. 

===========================
Options
===========================

``archive_command=<command>``
------
One command can be inserted with %path and %name arguments. %path is the fully qualified path of the commitlog segment to archive. %name is the filename of the commitlog. STDOUT, STDIN, or multiple commands cannot be executed. If multiple commands are required, add a pointer to a script in this option.

**Example:** archive_command=/bin/ln %path /backup/%name

**Default value:** blank

``restore_command=<command>``
------
One command can be inserted with %from and %to arguments. %from is the fully qualified path to an archived commitlog segment using the specified restore directories. %to defines the directory to the live commitlog location.

**Example:** restore_command=/bin/cp -f %from %to

**Default value:** blank

``restore_directories=<directory>``
------
Defines the directory to scan the recovery files into.

**Default value:** blank

``restore_point_in_time=<timestamp>``
------
Restore mutations created up to and including this timestamp in GMT in the format ``yyyy:MM:dd HH:mm:ss``.  Recovery will continue through the segment when the first client-supplied timestamp greater than this time is encountered, but only mutations less than or equal to this timestamp will be applied.

**Example:** 2020:04:31 20:43:12

**Default value:** blank

``precision=<timestamp_precision>``
------
Precision of the timestamp used in the inserts. Choice is generally MILLISECONDS or MICROSECONDS

**Default value:** MICROSECONDS
