SELECT * FROM myTable
 WHERE t > maxTimeuuid('2013-01-01 00:05+0000')
   AND t < minTimeuuid('2013-02-02 10:00+0000');
