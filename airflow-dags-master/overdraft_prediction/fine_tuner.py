from enum import Enum

import numpy as np


class ActionType(Enum):
    NEXT_BEST = 1
    ABSOLUTE = 2
    NEXT_BEST_DOWN = 3
    NEXT_BEST_UP = 4


class ThresholdType(Enum):
    LESS_THAN = 1
    MORE_THAN = 2


class TunerAction:
    def check(self, fineTuner, prob):
        pass

    def tune(self, fineTuner, pred_vec):
        pass

    def exec(self, fineTuner, pred_vec, val):
        prob = max(pred_vec)

        if self.check(fineTuner, prob):
            return self.tune(fineTuner, pred_vec)

        return val


class ForTunerAction:
    def pre_check(self, fineTuner, pred_vec, i):
        pass

    def check(self, fineTuner, pred_vec, i):
        pass

    def tune(self, fineTuner, pred_vec, i):
        pass

    def exec(self, fineTuner, pred_vec, val):
        if self.pre_check(fineTuner, pred_vec, val):
            for i in range(2, 6):
                if self.check(fineTuner, pred_vec, i):
                    return self.tune(fineTuner, pred_vec, i)

        return val


class LessThanNextBestTunerAction(TunerAction):
    def check(self, fineTuner, prob):
        return prob < fineTuner.threshold_value

    def tune(self, fineTuner, pred_vec):
        return pred_vec.argsort()[-2]


class MoreThanNextBestTunerAction(TunerAction):
    def check(self, fineTuner, prob):
        return prob > fineTuner.threshold_value

    def tune(self, fineTuner, pred_vec):
        return pred_vec.argsort()[-2]


class LessThanAbsoluteTunerAction(TunerAction):
    def check(self, fineTuner, prob):
        return prob < fineTuner.threshold_value

    def tune(self, fineTuner, pred_vec):
        return fineTuner.other_category


class MoreThanAbsoluteTunerAction(TunerAction):
    def check(self, fineTuner, prob):
        return prob > fineTuner.threshold_value

    def tune(self, fineTuner, pred_vec):
        return fineTuner.other_category


class LessThanNextBestDownTunerAction(ForTunerAction):
    def pre_check(self, fineTuner, pred_vec, i):
        prob = max(pred_vec)
        return prob < fineTuner.threshold_value

    def check(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i] < fineTuner.category_num

    def tune(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i]


class MoreThanNextBestDownTunerAction(ForTunerAction):
    def pre_check(self, fineTuner, pred_vec, i):
        prob = max(pred_vec)
        return prob > fineTuner.threshold_value

    def check(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i] < fineTuner.category_num

    def tune(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i]


class LessThanNextBestUpTunerAction(ForTunerAction):
    def pre_check(self, fineTuner, pred_vec, i):
        prob = max(pred_vec)
        return prob < fineTuner.threshold_value

    def check(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i] > fineTuner.category_num

    def tune(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i]


class MoreThanNextBestUpTunerAction(ForTunerAction):
    def pre_check(self, fineTuner, pred_vec, i):
        prob = max(pred_vec)
        return prob > fineTuner.threshold_value

    def check(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i] > fineTuner.category_num

    def tune(self, fineTuner, pred_vec, i):
        return pred_vec.argsort()[-i]


ACTIONS = {
    (ThresholdType.LESS_THAN, ActionType.NEXT_BEST): LessThanNextBestTunerAction(),
    (ThresholdType.MORE_THAN, ActionType.NEXT_BEST): MoreThanNextBestTunerAction(),
    (ThresholdType.LESS_THAN, ActionType.ABSOLUTE): LessThanAbsoluteTunerAction(),
    (ThresholdType.MORE_THAN, ActionType.ABSOLUTE): MoreThanAbsoluteTunerAction(),
    (ThresholdType.LESS_THAN, ActionType.NEXT_BEST_DOWN): LessThanNextBestDownTunerAction(),
    (ThresholdType.MORE_THAN, ActionType.NEXT_BEST_DOWN): MoreThanNextBestDownTunerAction(),
    (ThresholdType.LESS_THAN, ActionType.NEXT_BEST_UP): LessThanNextBestUpTunerAction(),
    (ThresholdType.MORE_THAN, ActionType.NEXT_BEST_UP): MoreThanNextBestUpTunerAction()
}


class FineTuner:
    def __init__(self, category_num, threshold_value, threshold_type, action_type, other_category=None):
        self.category_num = category_num
        self.threshold_value = threshold_value
        self.threshold_type = threshold_type
        self.action_type = action_type
        self.other_category = other_category
        if self.category_num == 0 and self.action_type == ActionType.NEXT_BEST_DOWN:
            raise ValueError("cannot choose lowest category for this action type")

        if self.category_num == 4 and self.action_type == ActionType.NEXT_BEST_UP:
            raise ValueError("cannot choose highest category for this action type")

    def tune(self, pred_vec):
        val = np.argmax(pred_vec)
        if val != self.category_num:
            return val

        actions = ACTIONS[self.threshold_type, self.action_type]
        return actions.exec(self, pred_vec, val)
