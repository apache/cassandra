create keyspace foo with replication = {'class':'SimpleStrategy', 'replication_factor':1};
use foo ;
create table atable ( pk int primary key, val int);
insert into atable (pk, val) VALUES ( 1,1);
insert into atable (pk, val) VALUES ( 2,2);
insert into atable (pk, val) VALUES ( 3,3);
insert into atable (pk, val) VALUES ( 4,4);
insert into atable (pk, val) VALUES ( 5,5);
insert into atable (pk, val) VALUES ( 6,6);
insert into atable (pk, val) VALUES ( 7,7);
insert into atable (pk, val) VALUES ( 8,8);
insert into atable (pk, val) VALUES ( 9,9);
insert into atable (pk, val) VALUES ( 10,10);



[
{"key": "5",
 "cells": [["","",1428529571195019],
           ["val","5",1428529571195019]]},
{"key": "10",
 "cells": [["","",1428529588242944],
           ["val","10",1428529588242944]]},
{"key": "1",
 "cells": [["","",1428529563371015],
           ["val","1",1428529563371015]]},
{"key": "8",
 "cells": [["","",1428529582362836],
           ["val","8",1428529582362836]]},
{"key": "2",
 "cells": [["","",1428529565275080],
           ["val","2",1428529565275080]]},
{"key": "4",
 "cells": [["","",1428529569331171],
           ["val","4",1428529569331171]]},
{"key": "7",
 "cells": [["","",1428529575898967],
           ["val","7",1428529575898967]]},
{"key": "6",
 "cells": [["","",1428529573027018],
           ["val","6",1428529573027018]]},
{"key": "9",
 "cells": [["","",1428529585667042],
           ["val","9",1428529585667042]]},
{"key": "3",
 "cells": [["","",1428529567379095],
           ["val","3",1428529567379095]]}
]



SSTable: test/data/legacy-sstables/ka/foo/foo-atable-ka-1
Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
Bloom Filter FP chance: 0,010000
Minimum timestamp: 1428529563371015
Maximum timestamp: 1428529588242944
SSTable max local deletion time: 2147483647
Compression ratio: 0.4
Estimated droppable tombstones: 0.0
SSTable Level: 0
Repaired at: 0
CommitLogPosition(segmentId=1428529465658, position=6481)
Estimated tombstone drop times:%n

