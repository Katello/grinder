import unittest

import sys
sys.path.append("../src/")
from grinder.RepoFetch import YumRepoGrinder

G_STATUS = ""
G_SIZE_LEFT = 0
G_ITEMS_LEFT = 0
def sampleCallBack(report):
    global G_SIZE_LEFT
    global G_ITEMS_LEFT
    global G_STATUS
    print "%s on <%s>, %s/%s items %s/%s bytes" % (report.status,
            report.item_name, report.items_left, report.items_total,
            report.size_left, report.size_total)
    if report.status not in ["STARTED", "FINISHED"]:
        assert(G_SIZE_LEFT > report.size_left)
        assert(G_ITEMS_LEFT > report.items_left)
    G_STATUS = report.status
    G_SIZE_LEFT = report.size_left
    G_ITEMS_LEFT = report.items_left

class TestRepoSync(unittest.TestCase):
    def __init__(self, arg):
        unittest.TestCase.__init__(self,arg)

    def testSync(self):
        channel_label = "rhel-i386-server-vt-5"
        repoURL = "http://mmccune.fedorapeople.org/pulp/fedora/13/x86_64/"
        parallel = 8 # number of threads to fetch with 
        repoGrinder = YumRepoGrinder("test_label_f13_updates", repoURL, parallel, 
                newest=True)
        report = repoGrinder.fetchYumRepo(callback=sampleCallBack)
        print "report = %s" % (report)
        self.assertTrue(G_STATUS == "FINISHED")
        self.assertTrue(G_SIZE_LEFT == 0)
        self.assertTrue(G_ITEMS_LEFT == 0)

if __name__ == '__main__':
    unittest.main()
