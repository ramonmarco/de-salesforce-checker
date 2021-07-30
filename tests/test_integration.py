import unittest
import subprocess


class TestIntegrationChecker(unittest.TestCase):

    def setUp(self):
        """ call the script and record its output """
        result = subprocess.run(["../src/main.py", "test_table", "2018-12-23"], shell=True, stdout=subprocess.PIPE)
        self.return_code = result.returncode
        self.output_lines = result.stdout.decode('utf-8').split('\n')

    def test_return_code(self):
        self.assertEqual(self.return_code, 0)

    def test_last_line_indicates_success(self):
        self.assertEqual(self.output_lines[-1], 'Writing to file')


if __name__ == '__main__':
    unittest.main()
