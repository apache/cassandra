# nosetests --tests=test.stress:Stress.ten_million_inserts

from hashlib import md5
from threading import Thread
from thread import get_ident
import time

from . import get_client, root, CassandraTester

class Inserter(Thread):
    def run(self):
        id = get_ident()
        self.count = 0
        client = get_client()
        client.transport.open()
        for i in xrange(0, 1000):
            data = md5(str(i)).hexdigest()
            for j in xrange(0, 1000):
                key = '%s.%s.%s' % (time.time(), id, j)
                client.insert('Table1', key, 'Standard1:A', data, i)
                client.insert('Table1', key, 'Standard1:B', data, i)
            self.count += 1000

class Stress(CassandraTester):
    runserver = False

    def ten_million_inserts(self):
        threads = []
        for i in xrange(0, 10):
            th = Inserter()
            threads.append(th)
            th.start()

        total = 0
        while True:
            time.sleep(1)
            total = sum(th.count for th in threads)
            file('/tmp/progress', 'w').write('%s\n' % str(total))
            if not [th for th in threads if th.isAlive()]:
                file('/tmp/progress', 'w').write('done -- %s\n' % str(total))
                break
