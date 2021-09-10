import sqlite3
import unittest
import check_fixity


class Tests(unittest.TestCase):

    def test_get_dirnames(self):
        db_conn = sqlite3.connect(':memory:')
        check_fixity.init_db(db_conn)
        num_dir_names = db_conn.execute('SELECT COUNT(*) FROM history').fetchone()[0]
        self.assertEqual(num_dir_names, 4096)
        dir_names = check_fixity.get_dir_names(db_conn, 3)
        self.assertEqual(dir_names, ['000', '001', '002'])
        check_fixity.set_dir_name_timestamp(db_conn, '001')
        dir_names = check_fixity.get_dir_names(db_conn, 3)
        self.assertEqual(dir_names, ['000', '002', '003'])



if __name__ == '__main__':
    unittest.main()
