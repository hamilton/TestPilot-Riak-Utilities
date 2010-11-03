import sqlite3
import unittest
from converter import parse_multipart
import os

class TestFileHandling(unittest.TestCase):
	def test_empty_file(self):
		self.assertRaises(ValueError, parse_multipart, 
			"test_data/empty.mp", 
			db_name = "empty.db")

class TestOutputMatches(unittest.TestCase):
	def setUp(self):
		parse_multipart("test_data/three.mp", db_name = "three.db")
		db = sqlite3.connect("three.db")
		self.c = db.cursor()
	
	def test_number_of_users(self, expected_count = 3):
		self.c.execute("""SELECT COUNT(*) from users;""")
		user_count = self.c.fetchone()[0]
		self.assertEqual(expected_count, user_count, \
			"There should only be %s users, but there were %s." % \
			(expected_count, user_count))
	
	def test_number_of_extensions(self, expected_count = 4):
		self.c.execute("""SELECT COUNT(*) from extensions;""")
		extension_count = self.c.fetchone()[0]
		self.assertEqual(expected_count, extension_count, \
			"There should only be %s extensions, but there were %s." % \
			 (expected_count, extension_count))
	
	def test_number_of_user_extension_mappings(self, expected_count = 4):
		self.c.execute("""SELECT COUNT(*) from user_extensions;""")
		extension_count = self.c.fetchone()[0]
		self.assertEqual(expected_count, extension_count, \
			"There should only be %s extension mappings, but there were %s."%\
			 (expected_count, extension_count))
	
	def test_extension_table_contents(self):
		self.c.execute("""SELECT * from extensions;""")
		#print self.c.fetchall()
	

if __name__ == "__main__":
	unittest.main()