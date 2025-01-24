CREATE TABLE ks.newtb1 LIKE ks.oldtb;

CREATE TABLE ks1.newtb1 LIKE ks.oldtb;

USE ks;

CREATE TABLE newtb1 LIKE oldtb;

CREATE TABLE IF NOT EXISTS newtb2 LIKE oldtb;

CREATE TABLE newtb3 LIKE oldtb WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
                               AND compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 }
                               AND cdc = true;

CREATE TABLE newtb4 LIKE oldtb WITH INDEXES;

CREATE TABLE newtb6 LIKE oldtb WITH INDEXES AND compaction = { 'class' : 'LeveledCompactionStrategy' };