VALUE_POSTFIX = '_VALUE'
THRESHOLD_POSTFIX = '_THRESHOLD'


class StatisticsTest:
    def __init__(self, field_prefix):
        self.field_prefix = field_prefix

    def test(self, row):
        pass

    def format(self, row):
        pass

    def get_test_value(self, row):
        return row[self.field_prefix]

    def get_test_absolute_value(self, row):
        return row[self.field_prefix + VALUE_POSTFIX]

    def format(self, row):
        pass


class ThresholdStatisticsTest(StatisticsTest):
    def get_threshold_field(self, row):
        return row[self.field_prefix + THRESHOLD_POSTFIX]

    def format(self, row):
        return "{:.2f}".format(self.get_test_value(row))


class PercentThresholdStatisticsTest(ThresholdStatisticsTest):
    def test(self, row):
        return abs(self.get_test_value(row)) <= self.get_threshold_field(row)

    def format(self, row):
        return "{:.2%} ({:,.2f})".format(self.get_test_value(row), self.get_test_absolute_value(row))


class EqualToThresholdStatisticsTest(ThresholdStatisticsTest):
    def test(self, row):
        return row[self.field_prefix] == self.get_threshold_field(row)


class LessThenThresholdStatisticsTest(ThresholdStatisticsTest):
    def test(self, row):
        return row[self.field_prefix] <= self.get_threshold_field(row)


class AlwaysPassStatisticsTest(StatisticsTest):
    def test(self, row):
        return True

    def format(self, row):
        return "{:,}".format(self.get_test_value(row))


TESTS = [
    EqualToThresholdStatisticsTest('GAP_BETWEEN_MYSQL_TO_PREDICTION_ALL_CUSTOMERS'),
    PercentThresholdStatisticsTest('CHANGE_IN_PRO_ELIGIBLE_ABSOLUTE_NUMB'),
    PercentThresholdStatisticsTest('CHANGE_IN_20'),
    PercentThresholdStatisticsTest('CHANGE_IN_40'),
    PercentThresholdStatisticsTest('CHANGE_IN_50'),
    AlwaysPassStatisticsTest('NEW_ELIGIBLE_BUCKET_20_NEW_PREDICTION'),
    AlwaysPassStatisticsTest('NEW_ELIGIBLE_BUCKET_40_NEW_PREDICTION'),
    AlwaysPassStatisticsTest('NEW_ELIGIBLE_BUCKET_50_NEW_PREDICTION'),
    EqualToThresholdStatisticsTest('MOST_FREQ_BUCKET_NEW_PREDICATION'),
    PercentThresholdStatisticsTest('CHANGE_IN_ELIGIBILITY'),
    PercentThresholdStatisticsTest('MONEY_RATIO'),
    AlwaysPassStatisticsTest('TOTAL_NEW_CREDIT'),
    LessThenThresholdStatisticsTest('NEW_ELIGIBLE_PREDICTION'),
    EqualToThresholdStatisticsTest('NEW_WITH_NEGATIVE_BALANCE'),
    LessThenThresholdStatisticsTest('REVOKED_NEW_PREDICTION'),
    EqualToThresholdStatisticsTest('TOTAL_ELIGIBLE_NOT_LEGIT_NEW_PREDICTION'),
    EqualToThresholdStatisticsTest('REVOKED_RECURRENT_NEW_PREDICATION')
]
