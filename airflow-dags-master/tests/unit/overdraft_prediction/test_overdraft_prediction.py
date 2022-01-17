from unittest import TestCase

import pandas as pd

from overdraft_prediction.overdraft_prediction import build_statistics_message
from overdraft_prediction.statistics_tests import EqualToThresholdStatisticsTest, LessThenThresholdStatisticsTest, \
    PercentThresholdStatisticsTest, AlwaysPassStatisticsTest


class Test(TestCase):
    def test_build_statistics_message(self):
        statistics_checks = [
            EqualToThresholdStatisticsTest('A'),
            LessThenThresholdStatisticsTest('B'),
            PercentThresholdStatisticsTest('C'),
            AlwaysPassStatisticsTest('D')
        ]
        df = pd.DataFrame([[15, 15, 10, 5, -0.5, 200, 2, 3000]],
                          columns=["A", "A_THRESHOLD", "B", "B_THRESHOLD", "C", "C_VALUE", "C_THRESHOLD", "D"])
        msg = build_statistics_message(statistics_checks, df)
        self.assertEqual(msg,
                         """*Prediction statistics tests result:* 
	:white_check_mark: A: 15.00
	:x: B: 10.00
	:white_check_mark: C: -50.00% (200.00)
	:white_check_mark: D: 3,000.0
""")
