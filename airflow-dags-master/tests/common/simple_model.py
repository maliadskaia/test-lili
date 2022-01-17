class SimpleModel:
    @staticmethod
    def predict(config, features_sets):
        res = []

        for features_set in features_sets:
            res.append([sum(features_set)])

        return res
