from unittest import TestCase

import numpy

from overdraft_prediction.fine_tuner import FineTuner, ActionType, ThresholdType


class TestFineTuner(TestCase):
    def test_predict_tuner__thresholdtype_less_than__actiontype_next_best(self):
        fine_tuner = FineTuner(3, 0.5, ThresholdType.LESS_THAN, ActionType.NEXT_BEST)
        self.assertEqual(fine_tuner.tune(numpy.array([0.05, 0.15, 0.10, 0.45, 0.25])), 4,
                         "category 3 is winning so expect it to return the next best")
        self.assertEqual(fine_tuner.tune(numpy.array([0.05, 0.15, 0.45, 0.25, 0.1])), 2,
                         "category 3 is not winning so expect it to return the highest")
        self.assertEqual(fine_tuner.tune(numpy.array([0.05, 0.15, 0.15, 0.55, 0.1])), 3,
                         "category 3 is winning but doesn't less then the threshold so expect it to return itself")

    def test_predict_tuner__thresholdtype_more_than__actiontype_next_best(self):
        fine_tuner = FineTuner(0, 0.3, ThresholdType.MORE_THAN, ActionType.NEXT_BEST)
        self.assertEqual(fine_tuner.tune(numpy.array([0.45, 0.05, 0.15, 0.25, 0.10])), 3,
                         "category 0 is winning so expect it to return the highest next best")
        self.assertEqual(fine_tuner.tune(numpy.array([0.05, 0.45, 0.15, 0.25, 0.10])), 1,
                         "category 0 is not winning so expect it to return the highest")
        self.assertEqual(fine_tuner.tune(numpy.array([0.25, 0.15, 0.20, 0.20, 0.15])), 0,
                         "category 0 is winning but doesn't more then the threshold so expect it to return itself")

    def test_predict_tuner__thresholdtype_more_than__actiontype_next_best_down(self):
        fine_tuner = FineTuner(2, 0.3, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_DOWN)
        self.assertEqual(fine_tuner.tune(numpy.array([0.1, 0.06, 0.5, 0.3, 0.04])), 0)
        try:
            FineTuner(0, 0.4, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_DOWN)
            self.fail()
        except ValueError as err:
            print(err)
            self.assertEqual(str(err), "cannot choose lowest category for this action type")
            pass

    def test_predict_tuner__thresholdtype_less_than__actiontype_next_best_down(self):
        fine_tuner = FineTuner(2, 0.4, ThresholdType.LESS_THAN, ActionType.NEXT_BEST_DOWN)
        self.assertEqual(fine_tuner.tune(numpy.array([0.1, 0.2, 0.32, 0.28, 0.1])), 1)
        try:
            FineTuner(0, 0.4, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_DOWN)
            self.fail()
        except ValueError as err:
            print(err)
            self.assertEqual(str(err), "cannot choose lowest category for this action type")
            pass

    def test_predict_tuner__thresholdtype_more_than__actiontype_next_best_up(self):
        fine_tuner = FineTuner(2, 0.3, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_UP)
        self.assertEqual(fine_tuner.tune(numpy.array([0.1, 0.06, 0.5, 0.3, 0.04])), 3)
        try:
            FineTuner(4, 0.4, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_UP)
            self.fail()
        except ValueError as err:
            print(err)
            self.assertEqual(str(err), "cannot choose highest category for this action type")
            pass

    def test_predict_tuner__thresholdtype_less_than__actiontype_next_best_up(self):
        fine_tuner = FineTuner(3, 0.42, ThresholdType.LESS_THAN, ActionType.NEXT_BEST_UP)
        self.assertEqual(fine_tuner.tune(numpy.array([0.06, 0.2, 0.3, 0.4, 0.04])), 4)
        try:
            FineTuner(4, 0.4, ThresholdType.MORE_THAN, ActionType.NEXT_BEST_UP)
            self.fail()
        except ValueError as err:
            print(err)
            self.assertEqual(str(err), "cannot choose highest category for this action type")
            pass

    def test_predict_tuner__thresholdtype_less_than__actiontype_absolute(self):
        fine_tuner = FineTuner(1, 0.55, ThresholdType.LESS_THAN, ActionType.ABSOLUTE, 3)
        self.assertEqual(fine_tuner.tune(numpy.array([0.15, 0.5, 0.25, 0.06, 0.04])), 3,
                         "category 1 is winning so expect it to return other_category")
        self.assertEqual(fine_tuner.tune(numpy.array([0.25, 0.05, 0.15, 0.45, 0.10])), 3,
                         "category 1 is not winning so expect it to return the highest")
        self.assertEqual(fine_tuner.tune(numpy.array([0.1, 0.6, 0.1, 0.1, 0.1])), 1,
                         "category 1 is winning but doesn't more then the threshold so expect it to return itself")

    def test_predict_tuner__thresholdtype_more_than__actiontype_absolute(self):
        fine_tuner = FineTuner(2, 0.35, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 0)
        self.assertEqual(fine_tuner.tune(numpy.array([0.05, 0.2, 0.4, 0.1, 0.25])), 0,
                         "category 2 is winning so expect it to return other_category")
        self.assertEqual(fine_tuner.tune(numpy.array([0.25, 0.05, 0.15, 0.45, 0.10])), 3,
                         "category 2 is not winning so expect it to return the highest")
        self.assertEqual(fine_tuner.tune(numpy.array([0.15, 0.25, 0.3, 0.1, 0.2])), 2,
                         "category 2 is winning but doesn't more then the threshold so expect it to return itself")
