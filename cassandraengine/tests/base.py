from unittest import TestCase

class BaseCassEngTestCase(TestCase):

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr), 
                "{} doesn't have attribute: {}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr), 
                "{} shouldn't have the attribute: {}".format(obj, attr))
