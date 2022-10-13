import unittest	
#from tap_tester import menagerie, connections	

from test_zuora_bookmarking_aqua import ZuoraBookmarking	


class ZuoraBookmarkingRest(ZuoraBookmarking):	
    """Test tap discovery mode with the REST API selected"""	

    @staticmethod	
    def name():	
        return "tap_tester_zuora_bookmarking_rest"	

    def test_brk_rest(self):	
        self.run_test("REST")