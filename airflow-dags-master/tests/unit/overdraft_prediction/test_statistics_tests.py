from unittest import TestCase

import pandas as pd

from overdraft_prediction.statistics_tests import StatisticsTest, ThresholdStatisticsTest, \
    PercentThresholdStatisticsTest, EqualToThresholdStatisticsTest, LessThenThresholdStatisticsTest, \
    AlwaysPassStatisticsTest

columns_with_value = ["A", "A_VALUE", "A_THRESHOLD"]
columns_no_value = ["A", "A_THRESHOLD"]


class Test(TestCase):
    def test_statistics_test(self):
        row = pd.DataFrame([[-0.5, 200, 2]], columns=columns_with_value).iloc[0]
        test = StatisticsTest('A')
        self.assertEqual(test.get_test_value(row), -0.5)
        self.assertEqual(test.get_test_absolute_value(row), 200)

    def test_threshold_statistics_test(self):
        row = pd.DataFrame([[-0.5, 200, 2]], columns=columns_with_value).iloc[0]
        test = ThresholdStatisticsTest('A')
        self.assertEqual(test.get_test_value(row), -0.5)
        self.assertEqual(test.get_test_absolute_value(row), 200)
        self.assertEqual(test.get_threshold_field(row), 2)

    def test_percent_threshold_statistics_test(self):
        test = PercentThresholdStatisticsTest('A')
        row = pd.DataFrame([[-0.5, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[0.5, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[2, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[-2, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[-3, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertFalse(test.test(row))

        row = pd.DataFrame([[3, 200, 2]], columns=columns_with_value).iloc[0]
        self.assertFalse(test.test(row))

        self.assertEqual(test.format(row), '300.00% (200.00)')

    def test_equal_to_threshold_statistics_test(self):
        test = EqualToThresholdStatisticsTest('A')
        row = pd.DataFrame([[100, 100]], columns=columns_no_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[100, 101]], columns=columns_no_value).iloc[0]
        self.assertFalse(test.test(row))

        row = pd.DataFrame([[100, 109]], columns=columns_no_value).iloc[0]
        self.assertFalse(test.test(row))

        self.assertEqual(test.format(row), '100.00')

    def test_less_then_threshold_statistics_test(self):
        test = LessThenThresholdStatisticsTest('A')
        row = pd.DataFrame([[100, 101]], columns=columns_no_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[100, 100]], columns=columns_no_value).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[100, 99]], columns=columns_no_value).iloc[0]
        self.assertFalse(test.test(row))

    def test_always_pass_statistics_test(self):
        test = AlwaysPassStatisticsTest('A')
        row = pd.DataFrame([[100]], columns=["A"]).iloc[0]
        self.assertTrue(test.test(row))

        row = pd.DataFrame([[10000000]], columns=["A"]).iloc[0]
        self.assertTrue(test.test(row))
        self.assertEqual(test.format(row), '10,000,000')


