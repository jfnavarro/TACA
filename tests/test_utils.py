""" Unit tests for the utils helper functions """

import os
import shutil
import tempfile
import unittest
from taca.utils import misc, filesystem

class TestMisc():  
    """ Test class for the misc functions """

    @classmethod
    def setUpClass(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_misc")
        self.hashfile = os.path.join(self.rootdir,'test_hashfile')
        with open(self.hashfile,'w') as fh:
            fh.write("This is some contents\n")
        self.hashfile_digests = {
            'SHA256':
                '4f075ae76b480bb0200dab01cd304f4045e04cd2b73e88b89549e5ac1627f222',
            'MD5':
                'c8498fc299bc3e22690045f1b62ce4e9',
            'SHA1':
                '098fb272dfdae2ea1ba57c795dd325fa70e3c3fb'}
            
    @classmethod        
    def tearDownClass(self):
        shutil.rmtree(self.rootdir)
    
    # Test generator for different hashing algorithms
    def test_hashfile(self):
        for alg,obj in self.hashfile_digests.items():
            yield self.check_hash, alg, obj
    
    def test_hashfile_dir(self):
        """Hash digest for a directory should be None"""   
        assert misc.hashfile(self.rootdir) is None
    
    def test_multiple_hashfile_calls(self):
        """ Ensure that the hasher object is cleared between subsequent calls
        """
        assert misc.hashfile(self.hashfile,hasher='sha1') == misc.hashfile(self.hashfile,'sha1')
        
    def check_hash(self, alg, exp):
        assert misc.hashfile(self.hashfile,hasher=alg) == exp
        
class TestFilesystem(unittest.TestCase):
    """ Test class for the filesystem functions """

    def setUp(self):
        self.rootdir = tempfile.mkdtemp(prefix="test_taca_filesystem")
        
    def tearDown(self):
        shutil.rmtree(self.rootdir)
    
    def test_crete_folder1(self):
        """ Ensure that a non-existing folder is created """
        target_folder = os.path.join(self.rootdir,"target-non-existing")
        self.assertTrue(
            filesystem.create_folder(target_folder),
            "A non-existing target folder could not be created")
        self.assertTrue(
            os.path.exists(target_folder),
            "A non-existing target folder was not created \
            but method returned True"
        )
    
    def test_crete_folder2(self):
        """ Ensure that an existing folder is detected """
        self.assertTrue(
            filesystem.create_folder(self.rootdir),
            "A pre-existing target folder was not detected")
    
    def test_crete_folder3(self):
        """ Ensure that a non-existing parent folder is created """
        target_folder = os.path.join(
            self.rootdir,
            "parent-non-existing",
            "target-non-existing")
        self.assertTrue(
            filesystem.create_folder(target_folder),
            "A non-existing parent and target folder could not be created")
        self.assertTrue(
            os.path.exists(target_folder),
            "A non-existing parent folder was not created \
            but method returned True"
        )