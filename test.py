# standard libraries
import unittest

# third-party libarries
import pandas as pd

# local libraries
import compare, identify

class TestComparison(unittest.TestCase):

    def test_title_comparison(self):
        # See https://www.worldcat.org/search?qt=worldcat_org_all&q=hound+of+the+baskervilles
        left = "The hound of the Baskervilles"
        right = "HOUND OF THE BASKERVILLES."
        compare_to_title = compare.create_compare_func([left], 85)
        result = compare_to_title(right)
        self.assertTrue(result)

    def test_publisher_comparison(self):
        left = "University of MI Press"
        right = "Univ. of Michigan Press"
        compare_to_publisher = compare.create_compare_func([left], 85, [compare.normalize_univ])
        result = compare_to_publisher(right)
        self.assertTrue(result)


unittest.main()