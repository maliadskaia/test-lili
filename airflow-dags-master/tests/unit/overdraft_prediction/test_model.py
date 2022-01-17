import numpy as np
import xgboost as xgb
from numpy.testing import assert_array_equal
from unittest import TestCase

from overdraft_prediction.fine_tuner import FineTuner, ThresholdType, ActionType
from overdraft_prediction.model import ODModel, NUM_FEATS, IS_CLOSE_EXPIRATION_IDX
from tests.unit.common.consts import config

extra_feat = [0] * (NUM_FEATS - 6)
extra_feat2 = [1] * (NUM_FEATS - 6)
FEATURES = [[1., 1., 0., 550.5, 3906.5, 4457.,
             2522.47, 1921.09, 0., 17.6, 0., 0.,
             4461.16, 47., 118.767, 0., 1., 0.,
             1., 0., 207., 0., 1., 1.,
             94.918],
            [1., 1., 6., 0., 4728.5, 4728.5,
             1750., 2981.39, 0., 0., 320., 0.,
             5051.39, 36., 110.274, 0., 0., 0.,
             0., 0., 144., 1., 1., 2.,
             140.316],
            [1., 1., 24., 692.99, 6974.68, 7667.67,
             0., 4285.21, 0., 896.32, 2085., 0.,
             7266.53, 60., 50.645, 0., 0., 0.,
             0., 0., 129., 1., 0., 0.,
             121.109],
            [1., 1., 0., 43.5, 5029.74, 5073.24,
             4585.37, 422.96, 0., 0., 0., 0.,
             5008.33, 10., 172.872, 0., 0., 0.,
             1., 1., 78., 0., 2., 3.,
             500.833],
            [1., 1., 0., 0., 2658.99, 2658.99,
             3160., 13.2, 0., 0., 0., 0.,
             3173.2, 13., 247.003, 0., 0., 0.,
             1., 0., 204., 0., 0., 1.,
             244.092],
            [1., 1., 0., 0., 1530.27, 1530.27,
             1148.57, 255.02, 0., 0., 0., 0.,
             1403.59, 8., 31.517, 0., 0., 0.,
             0., 0., 99., 1., 1., 1.,
             175.449],
            [1., 1., 0., 0., 2476.09, 2476.09,
             0., 1257.26, 0., 0., 1220., 0.,
             2477.26, 49., 22.59, 0., 0., 0.,
             0., 0., 54., 1., 0., 0.,
             50.556],
            [1., 1., 0., 283., 3487.37, 3770.37,
             250., 3524.5, 0., 0., 0., 0.,
             3774.5, 5., 180.203, 0., 0., 0.,
             0., 0., 151., 1., 7., 11.,
             754.9],
            [1., 1., 0., 2614.47, 22117.57, 24732.04,
             1960.81, 3578.7, 0., 0., 0., 0.,
             5539.51, 31., 8896.735, 1., 1., 0.,
             0., 0., 127., 0., 3., 4.,
             178.694],
            [1., 1., 2., 1475.45, 330.69, 1806.14,
             0., 1661.99, 0., 0., 100., 0.,
             1762.16, 21., 34.005, 0., 0., 0.,
             0., 0., 110., 1., 0., 0.,
             83.912],
            [1., 1., 5., 43.75, 1221.53, 1265.28,
             0., 260.8, 0., 3450., 0., 0.,
             3710.8, 6., -63.893, 0., 1., 0.,
             0., 0., 58., 0., 5., 8.,
             618.467],
            [1., 1., 0., 0., 7962.79, 7962.79,
             1970.45, 6468.88, 0., 0., 0., 0.,
             8439.34, 42., 321.314, 1., 1., 0.,
             0., 0., 192., 0., 0., 0.,
             200.937],
            [1., 1., 0., 33.75, 4327.58, 4361.33,
             2.5, 3733.15, 0., 0., 548.92, 0.,
             4284.57, 36., 53.449, 0., 1., 0.,
             0., 0., 194., 0., 0., 0.,
             119.016],
            [1., 1., 0., 700.99, 2947.98, 3648.97,
             4538.69, 444.71, 0., 0., 0., 0.,
             4983.4, 14., 398.924, 0., 0., 0.,
             0., 0., 60., 1., 0., 1.,
             355.957],
            [1., 1., 2., 0., 1093.55, 1093.55,
             916., 0., 0., 0., 0., 0.,
             916., 2., 51.708, 0., 0., 0.,
             1., 0., 330., 0., 8., 20.,
             458.],
            [1., 1., 0., 376.5, 1527.09, 1903.59,
             1485., 499.7, 0., 0., 0., 0.,
             1984.7, 15., 50.628, 0., 0., 0.,
             1., 0., 134., 0., 0., 1.,
             132.313],
            [1., 1., 0., 0., 3895.13, 3895.13,
             3191.32, 440.55, 0., 0., 0., 0.,
             3631.87, 11., 179.218, 0., 0., 0.,
             0., 0., 99., 1., 0., 1.,
             330.17],
            [1., 1., 0., 403., 2689.47, 3092.47,
             930.94, 460.19, 0., 0., 300., 0.,
             1691.13, 18., 563.591, 1., 1., 0.,
             0., 0., 45., 0., 1., 1.,
             93.952],
            [1., 1., 0., 0., 545., 545.,
             550., 0., 0., 0., 0., 0.,
             551.75, 3., 138.931, 0., 0., 0.,
             0., 0., 38., 1., 6., 13.,
             183.917],
            [1., 1., 0., 0., 771.62, 771.62,
             413., 357.21, 0., 0., 0., 0.,
             770.21, 11., 34.125, 0., 0., 0.,
             1., 0., 50., 0., 16., 16.,
             70.019],
            [1., 1., 23., 1453.5, 3870.55, 5324.05,
             3078.13, 2367.99, 0., 2118.85, 533., 0.,
             8099.07, 21., 318.042, 1., 0., 1.,
             1., 0., 154., 0., 0., 5.,
             385.67],
            [1., 1., 0., 0., 2422.97, 2422.97,
             2733., 186.29, 0., 0., 0., 0.,
             2919.29, 12., 100.604, 0., 0., 1.,
             1., 0., 114., 0., 1., 1.,
             243.274],
            [1., 1., 0., 412.8, 2425.18, 2837.98,
             1141.14, 847., 0., 0., 0., 0.,
             1989.01, 18., 604.174, 0., 0., 1.,
             1., 0., 175., 0., 1., 1.,
             110.501],
            [1., 1., 0., 21.79, 3944.32, 3966.11,
             2655., 1325.61, 0., 0., 0., 0.,
             3980.61, 28., 126.317, 0., 0., 0.,
             1., 0., 140., 0., 0., 1.,
             142.165],
            [1., 1., 0., 3091.5, 2692.41, 5783.91,
             3337., 283.51, 0., 0., 1800., 0.,
             5420.51, 16., 159.165, 0., 0., 0.,
             1., 0., 219., 0., 0., 1.,
             338.782],
            [1., 1., 9., 360., 593.12, 953.12,
             675.84, 279.96, 0., 0., 0., 0.,
             955.8, 13., 31.58, 0., 0., 0.,
             0., 0., 160., 1., 0., 3.,
             73.523],
            [1., 1., 15., 86., 5342.15, 5428.15,
             421., 4899.9, 0., 0., 0., 0.,
             5320.9, 20., 76.368, 0., 0., 0.,
             1., 0., 158., 0., 14., 15.,
             266.045],
            [1., 1., 0., 203., 5043.07, 5246.07,
             1206.44, 2590.62, 0., 1450., 0., 0.,
             5247.06, 31., 70.393, 0., 0., 0.,
             1., 0., 115., 0., 0., 0.,
             169.26],
            [1., 1., 0., 406., 11599.44, 12005.44,
             14043., 0., 0., 0., 0., 0.,
             14043.27, 5., 3180.97, 0., 0., 0.,
             1., 0., 323., 0., 0., 1.,
             2808.654],
            [1., 1., 0., 0., 10222.68, 10222.68,
             0., 9770.06, 400., 0., 0., 0.,
             10170.06, 19., 482.699, 0., 0., 0.,
             0., 0., 33., 1., 0., 0.,
             535.266],
            [1., 1., 0., 0., 870.67, 870.67,
             1050., 0., 0., 0., 0., 0.,
             1050.01, 2., 435.783, 0., 0., 0.,
             0., 0., 55., 1., 0., 13.,
             525.005]]


class SimpleModel:
    def predict_proba(self, features_sets):
        res = []

        for features_set in features_sets:
            arr = [0, 0, 0, 0, 0]
            arr[int(sum(features_set)) % 5] = 1
            res.append(arr)

        return res

    def predict(self, features_sets):
        res = []
        for features_set in features_sets:
            res.append(int(sum(features_set)) % 5)

        return res


class SimpleModel2:
    def predict_proba(self, features_sets):
        res = []

        for features_set in features_sets:
            arr = [0, 0]
            arr[int(sum(features_set)) % 2] = 1
            res.append(arr)

        return res

    def predict(self, features_sets):
        res = []
        for features_set in features_sets:
            res.append(int(sum(features_set)) % 2)

        return res


class TestODModel(TestCase):
    def setUp(self):
        self.model = SimpleModel()
        self.model2 = SimpleModel2()

    def test_remove_extra_features(self):
        features = [
            [0., 1., 35, 0, 45, 6., 1, 2, 6, 4, 8, 9, 10, 9, 8, 7] + extra_feat[11:],
            [1., 1., 25, 1, -65, 7., 0, 2, 6, 5, 7, 11, 6, 4, 2, 9] + extra_feat[11:]
        ]

        expected_result = [
            [6., 1, 2, 6, 4, 8, 9, 10, 9, 8] + extra_feat[13:],
            [7., 0, 2, 6, 5, 7, 11, 6, 4, 2] + extra_feat[13:]
        ]

        odmodel = ODModel(model_money_in=self.model, model_reject=self.model2)
        assert_array_equal(odmodel.remove_extra_features(features), expected_result)

    def test_remove_features_for_rejection(self):
        features = [
            [0., 1., 35, 0, 45, 6., 0, 8] + extra_feat[2:],
            [1., 1., 25, 1, -65, 7., 0, 11] + extra_feat[2:]
        ]

        expected_result = [
            [0, 45, 6., 0, 8] + extra_feat[5:],
            [1, -65, 7., 0, 11] + extra_feat[5:]
        ]

        odmodel = ODModel(model_money_in=self.model, model_reject=self.model2)
        assert_array_equal(odmodel.remove_features_for_rejection(features), expected_result)

    def test_filter_rejected_users(self):
        features_sets = []

        for account_active in [0, 1]:
            for pro_user in [0, 1]:
                for total_money_in in [499, 500.1]:
                    for signup_days in [29, 30]:
                        features_sets.append(
                            [account_active, pro_user, signup_days, 0, 0, total_money_in] + extra_feat2)

        odmodel = ODModel(self.model)
        for i, features_set in enumerate(features_sets):
            print(features_set[0:7])
            if i == 0:
                self.assertEqual("not active and not pro and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 1:
                self.assertEqual("not active and not pro and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 2:
                self.assertEqual("not active and not pro and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 3:
                self.assertEqual("not active and not pro" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 4:
                self.assertEqual("not active and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 5:
                self.assertEqual("not active and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 6:
                self.assertEqual("not active and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 7:
                self.assertEqual("not active" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 8:
                self.assertEqual("not pro and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 9:
                self.assertEqual("not pro and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 10:
                self.assertEqual("not pro and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 11:
                self.assertEqual("not pro" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 12:
                self.assertEqual("less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 13:
                self.assertEqual("money in <500" \
                                 , odmodel.filter_rejected_users(features_set))
            elif i == 14:
                self.assertEqual("less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features_set))
            else:
                self.assertEqual("", odmodel.filter_rejected_users(features_set))

    def test_filter_rejected_users_with_reason(self):
        features_sets = []

        for account_active in [0, 1]:
            for pro_user in [0, 1]:
                for total_money_in in [499, 500.1]:
                    for signup_days in [29, 30]:
                        features_sets.append(
                            [account_active, pro_user, signup_days, 0, 0, total_money_in] + extra_feat2)

        odmodel = ODModel(self.model)
        self.assertEqual("model rejection" \
                         , odmodel.filter_rejected_users(features=features_sets[0], added_reason=1))
        for i, features_set in enumerate(features_sets):
            print(features_set[0:7])
            if i == 0:
                self.assertEqual("not active and not pro and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 1:
                self.assertEqual("not active and not pro and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 2:
                self.assertEqual("not active and not pro and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 3:
                self.assertEqual("not active and not pro" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 4:
                self.assertEqual("not active and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 5:
                self.assertEqual("not active and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 6:
                self.assertEqual("not active and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 7:
                self.assertEqual("not active" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 8:
                self.assertEqual("not pro and less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 9:
                self.assertEqual("not pro and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 10:
                self.assertEqual("not pro and less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 11:
                self.assertEqual("not pro" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 12:
                self.assertEqual("less than 30 days from signup and money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 13:
                self.assertEqual("money in <500" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            elif i == 14:
                self.assertEqual("less than 30 days from signup" \
                                 , odmodel.filter_rejected_users(features=features_set, added_reason=0))
            else:
                self.assertEqual("", odmodel.filter_rejected_users(features=features_set, added_reason=0))

    def test_fine_tune_not_list(self):
        fine_tuner = FineTuner(3, 0.5, ThresholdType.LESS_THAN, ActionType.NEXT_BEST)
        odmodel = ODModel(model_money_in=self.model, finetuner=fine_tuner)
        self.assertEqual(odmodel.finetune(3, np.array([0.05, 0.15, 0.10, 0.45, 0.25])), 4,
                         "since category 3 is highest and stands in the threshold, expect to get the next best")
        self.assertEqual(odmodel.finetune(3, np.array([0.05, 0.05, 0.10, 0.55, 0.25])), 3,
                         "expect it to not change, since 3 is the highest, but not the stands in threshold condition")

    def test_fine_tune_is_list(self):
        fine_tuner1 = FineTuner(1, 0.5, ThresholdType.LESS_THAN, ActionType.NEXT_BEST)
        fine_tuner2 = FineTuner(2, 0.45, ThresholdType.LESS_THAN, ActionType.ABSOLUTE, 4)
        fine_tuner3 = FineTuner(2, 0.5, ThresholdType.MORE_THAN, ActionType.NEXT_BEST)
        fine_tuner4 = FineTuner(3, 0.4, ThresholdType.MORE_THAN, ActionType.NEXT_BEST)
        odmodel = ODModel(model_money_in=self.model, finetuner=[fine_tuner1, fine_tuner2, fine_tuner3, fine_tuner4])
        self.assertEqual(odmodel.finetune(1, np.array([0.2, 0.4, 0.3, 0.05, 0.05])), 2,
                         "since category 0 is highest and stands in the threshold, expect to get the next best")
        self.assertEqual(odmodel.finetune(2, np.array([0.1, 0.2, 0.40, 0.25, 0.05])), 4,
                         "since category 2 is the highest and stands in the threshold, expect to get 4 as absolute")
        self.assertEqual(odmodel.finetune(3, np.array([0.1, 0.2, 0.30, 0.35, 0.05])), 3,
                         "since category 3 is the highest, but does not stand in the threshold, expect no change")
        self.assertEqual(odmodel.finetune(3, np.array([0.1, 0.15, 0.25, 0.45, 0.05])), 2,
                         "since category 3 is the highest and stands in the threshold, expect to get the next best")
        self.assertEqual(odmodel.finetune(2, np.array([0.05, 0.15, 0.47, 0.03, 0.25])), 2,
                         "since category 2 is the highest, but does not stand in the threshold, expect no change")
        self.assertEqual(odmodel.finetune(4, np.array([0.05, 0.05, 0.2, 0.3, 0.4])), 4,
                         "since category 4 is the highest, but does not have finetune, expect no change")

    def test_fine_tune_is_empty_list(self):
        try:
            ODModel(model_money_in=None, finetuner=[])
            self.fail()
        except ValueError as err:
            self.assertEqual(str(err), "finetuner cannot be empty list")
            pass

    def test_feature_dims_ok(self):
        features = [
            [1, 1, 30, 1, 505] + extra_feat,
            [1, 1, 30, 0, 505] + extra_feat]
        odmodel = ODModel(model_money_in=self.model)
        try:
            odmodel.predict(config, features)
            self.fail()
        except ValueError as err:
            self.assertEqual(str(err), "Input feature dimension does not match expected dimension." \
                                       "Expected " + str(NUM_FEATS) + " dimensions, " \
                                                                      "received " + str(
                (len(features[0]))) + " dimensions")
            pass

    def test_predict_reject_sometimes(self):
        features = [
            [1, 1, 30, 0, 0, 508] + extra_feat2,
            [1., 1., 30, 1, 0, 509] + extra_feat2]
        features[0][IS_CLOSE_EXPIRATION_IDX]=0
        features[1][IS_CLOSE_EXPIRATION_IDX]=0

        fine_tuner1 = FineTuner(2, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 3)
        fine_tuner2 = FineTuner(3, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 4)
        odmodel1 = ODModel(model_money_in=self.model)
        odmodel2 = ODModel(model_money_in=self.model, finetuner=fine_tuner1)
        odmodel3 = ODModel(model_money_in=self.model, finetuner=fine_tuner2)
        odmodel4 = ODModel(model_money_in=self.model, finetuner=[fine_tuner1, fine_tuner2])
        res1 = odmodel1.predict(config, features)
        res2 = odmodel2.predict(config, features)
        res3 = odmodel3.predict(config, features)
        res4 = odmodel4.predict(config, features)

        self.assertEqual(res1[0], [40, '2021-08-05', ], "no finetune selected, expect no change")
        self.assertEqual(res1[1], [60, '2021-08-05', ], "no finetune selected, expect no change")

        self.assertEqual(res2[0], [60, '2021-08-05', ],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res2[1], [60, '2021-08-05', ],
                         "finetune was not activated so output did not change")

        self.assertEqual(res3[0], [40, '2021-08-05', ],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res3[1], [100, '2021-08-05', ],
                         "finetune was activated so output should change")

        self.assertEqual(res4[0], [60, '2021-08-05', ],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res4[1], [100, '2021-08-05', ],
                         "finetune was activated so output should change")

    def test_predict_reject_all(self):
        features = [
            [1., 1., 30, 0, 0, 495.] + extra_feat2,
            [1., 0., 30, 0, 0, 502.] + extra_feat2]
        fine_tuner1 = FineTuner(0, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 3)
        fine_tuner2 = FineTuner(2, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 4)
        odmodel1 = ODModel(model_money_in=self.model)
        odmodel2 = ODModel(model_money_in=self.model, finetuner=fine_tuner1)
        odmodel3 = ODModel(model_money_in=self.model, finetuner=fine_tuner2)
        odmodel4 = ODModel(model_money_in=self.model, finetuner=[fine_tuner1, fine_tuner2])
        res1 = odmodel1.predict(config, features)
        res2 = odmodel2.predict(config, features)
        res3 = odmodel3.predict(config, features)
        res4 = odmodel4.predict(config, features)

        self.assertEqual(res1[0], [0, '2021-08-05', "money in <500"], "no finetune selected, expect no change")
        self.assertEqual(res1[1], [0, '2021-08-05', "not pro"], "no finetune selected, expect no change")

        self.assertEqual(res2[0], [0, '2021-08-05', "money in <500"],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res2[1], [0, '2021-08-05', "not pro"],
                         "auto reject does not care about finetune. Thus we get same result")

        self.assertEqual(res3[0], [0, '2021-08-05', "money in <500"],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res3[1], [0, '2021-08-05', "not pro"],
                         "auto reject does not care about finetune. Thus we get same result")

        self.assertEqual(res4[0], [0, '2021-08-05', "money in <500"],
                         "auto reject does not care about finetune. Thus we get same result")
        self.assertEqual(res4[1], [0, '2021-08-05', "not pro"],
                         "auto reject does not care about finetune. Thus we get same result")


    def test_predict_never_reject(self):
        features = [
            [1., 1., 30, 0, 0, 502.] + extra_feat2,
            [1., 1., 30, 0, 0, 503.] + extra_feat2]
        features[0][IS_CLOSE_EXPIRATION_IDX] = 0
        features[1][IS_CLOSE_EXPIRATION_IDX] = 0
        fine_tuner1 = FineTuner(1, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 3)
        fine_tuner2 = FineTuner(2, 0.5, ThresholdType.MORE_THAN, ActionType.ABSOLUTE, 4)
        odmodel1 = ODModel(model_money_in=self.model)
        odmodel2 = ODModel(model_money_in=self.model, finetuner=fine_tuner1)
        odmodel3 = ODModel(model_money_in=self.model, finetuner=fine_tuner2)
        odmodel4 = ODModel(model_money_in=self.model, finetuner=[fine_tuner1, fine_tuner2])
        res1 = odmodel1.predict(config, features)
        res2 = odmodel2.predict(config, features)
        res3 = odmodel3.predict(config, features)
        res4 = odmodel4.predict(config, features)

        self.assertEqual(res1[0], [20, '2021-08-05', ], "no finetune selected, expect no change")
        self.assertEqual(res1[1], [40, '2021-08-05', ], "no finetune selected, expect no change")

        self.assertEqual(res2[0], [60, '2021-08-05', ],
                         "finetune was activated so output should change")
        self.assertEqual(res2[1], [40, '2021-08-05', ],
                         "no finetune selected, expect no change")

        self.assertEqual(res3[0], [20, '2021-08-05', ],
                         "no finetune selected, expect no change")
        self.assertEqual(res3[1], [100, '2021-08-05', ],
                         "finetune was activated so output should change")

        self.assertEqual(res4[0], [60, '2021-08-05', ],
                         "finetune was activated so output should change")
        self.assertEqual(res4[1], [100, '2021-08-05', ],
                         "finetune was activated so output should change")


    def test_post_prediction_filtration_no_change(self):
        features = [
            [1., 1., 30, 1, 0, 505] + extra_feat,
            [1., 1., 30, 1, -25, 505] + extra_feat,
            [1., 1., 30, 1, -115, 505] + extra_feat]

        odmodel = ODModel(model_money_in=self.model)

        for i in range(0, 5):
            self.assertEqual(odmodel.post_prediction_filtration(features[0], i), i, "no change for balance 0")

        self.assertEqual(odmodel.post_prediction_filtration(features[1], 0), 0,
                         "no overdraft for more than 15 USD difference")
        self.assertEqual(odmodel.post_prediction_filtration(features[1], 1), 1,
                         "overdraft remains the same, since we have less than 15 USD difference")
        self.assertEqual(odmodel.post_prediction_filtration(features[1], 2), 2,
                         "overdraft remains the same, since we have less than 15 USD difference")
        self.assertEqual(odmodel.post_prediction_filtration(features[1], 3), 3,
                         "overdraft remains the same, since we have less than 15 USD difference")
        self.assertEqual(odmodel.post_prediction_filtration(features[1], 4), 4,
                         "overdraft remains the same, since we have less than 15 USD difference")
        self.assertEqual(odmodel.post_prediction_filtration(features[2], 4), 4,
                         "overdraft remains the same, since we have less than 15 USD difference")


    def test_post_prediction_filtration_change(self):
        features = [1., 1., 30, 1, -116, 505] + extra_feat
        odmodel = ODModel(model_money_in=self.model)
        for i in range(0, 5):
            self.assertEqual(odmodel.post_prediction_filtration(features, i), 0,
                             "no overdraft for more than 15 USD difference")


    def test_predict_proba(self):
        features = [
            [1., 1., 30, 1, 0, 501, 1, 1, 1, 1, 1, 1, 1, 1, 1, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]
        odmodel = ODModel(model_money_in=self.model)
        self.assertEqual(odmodel.predict_proba(config, features), [[1, 0, 0, 0, 0]])


# If needed to run one specific test, run this code:
if __name__ == '__main__':
    testush = TestODModel()
    testush.test_predict_never_reject()
