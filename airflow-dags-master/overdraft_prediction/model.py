import datetime
import overdraft_prediction.index2reason as rsn
import numpy as np

REJECTED = 1
ACTIVE_USER_IDX = 0
IS_PRO_CUSTOMER_IDX = 1
TIME_FROM_SIGNUP_DATE_IDX = 2
IS_PRO_SINCE_SIGNUP_IDX = 3
CURRENT_BALANCE_IDX = 4
TOTAL_MONEY_IN_IDX = 5
TOTAL_MONEY_IN_WEEK3_IDX = 11
TOTAL_MONEY_IN_COUNT_WEEK3_IDX = 12
TOTAL_MONEY_IN_WEEK4_IDX = 13
TOTAL_MONEY_IN_COUNT_WEEK4_IDX = 14
OVERDRAFT_LIMIT_IDX = 15
HAD_NEGATIVE_BALANCE_IDX = 16
TIME_FROM_LAST_TRANSACTION_IDX = 55
TIME_FROM_LAST_MONEY_IN_IDX = 56
IS_RECURRING_DIRECT_DEPOSIT_IDX = 58
IS_CLOSE_EXPIRATION_IDX = 59
NUM_FEATS = 518
REJECTION_PREDICTION_THRESHOLD = 0.28

rejection_model_list = [IS_CLOSE_EXPIRATION_IDX, IS_RECURRING_DIRECT_DEPOSIT_IDX, OVERDRAFT_LIMIT_IDX,
                        TIME_FROM_SIGNUP_DATE_IDX, IS_PRO_CUSTOMER_IDX, ACTIVE_USER_IDX]

extra_features_removal_list = [IS_CLOSE_EXPIRATION_IDX, IS_RECURRING_DIRECT_DEPOSIT_IDX, OVERDRAFT_LIMIT_IDX,
                               CURRENT_BALANCE_IDX, IS_PRO_SINCE_SIGNUP_IDX, TIME_FROM_SIGNUP_DATE_IDX,
                               IS_PRO_CUSTOMER_IDX, ACTIVE_USER_IDX]

THRESHOLD_OF_DIFF_BETWEEN_NEGATIVE_BALANCE_AND_OVERDRAFT = -15

MINIMUM_MONEY_IN_FOR_OVERDRAFT = 500

MINIMUN_DAYS_FROM_SIGNUP = 30

predictions_to_overdraft = {
    0: 0,
    1: 20,
    2: 40,
    3: 60,
    4: 100
}

class ODModel:
    # self.model = xgb.sklearn.XGBClassifier
    def __init__(self, model_money_in, model_reject=None, finetuner=None, explainer=None):
        self.model_money_in = model_money_in
        self.model_reject = model_reject
        if not finetuner and isinstance(finetuner, list):
            raise ValueError("finetuner cannot be empty list")

        self.finetuner = finetuner
        self.explainer = explainer

    def predict(self, config, features):
        if NUM_FEATS != len(features[0]):
            raise ValueError("Input feature dimension does not match expected dimension." \
                             "Expected " + str(NUM_FEATS) + " dimensions, " \
                             "received " + str((len(features[0]))) + " dimensions")

        rej_preds = None
        preds_probas = None

        expiration_date = (config.get_dag_run_start_date_date() + datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        if self.model_reject is not None:
            feat_rej = self.remove_features_for_rejection(features)
            rej_preds = self.rejection_predict(feat_rej)

        feat_in = self.remove_extra_features(features)

        if self.finetuner is not None:
            preds_probas = self.model_money_in.predict_proba(feat_in)

        predictions = self.model_money_in.predict(feat_in)
        res = []
        inp = None

        for i, prediction in enumerate(predictions):
            out = ""

            if rej_preds is not None:
                inp = rej_preds[i]

            if len(self.filter_rejected_users(features[i], inp)) != 0:
                res.append([0, expiration_date, self.filter_rejected_users(features[i], inp)])
                continue

            if self.is_keep_old_balance(features[i], prediction):
                res.append([features[i][OVERDRAFT_LIMIT_IDX], expiration_date, "keep old balance-up"])
                continue


            #this logic is added since we do not want to give 0 prediction to recurrent DD users
            new_prediction = prediction
            if new_prediction == 0:
                new_prediction = 1

            if self.explainer is not None:
                shap = self.explainer.shap_values(feat_in[i])
                ix = np.argmax(np.abs(shap[prediction]))
                explainer_reason = rsn.index_dict[ix]
                out = f"[pred:{str(new_prediction)} reason - {str(explainer_reason)}]"

            #if finetune object is on, do we still want the new prediction of 1 to overwrite?
            if self.finetuner is not None:
                new_prediction = self.finetune(new_prediction, preds_probas[i])

            revised_prediction = self.post_prediction_filtration(features[i], new_prediction)
            if out == "":
                res.append([predictions_to_overdraft[revised_prediction], expiration_date, ])
            else:
                res.append([predictions_to_overdraft[revised_prediction], expiration_date, out])

        return res

    def finetune(self, prediction, preds_proba):
        if isinstance(self.finetuner, list):
            for j in range(0, len(self.finetuner)):
                if prediction != self.finetuner[j].tune(preds_proba):
                    return self.finetuner[j].tune(preds_proba)

            return prediction

        return self.finetuner.tune(preds_proba)

    def filter_rejected_users(self, features, added_reason=None):
        #This function filters out users that either do not
        #pass first model rejection or don't pass some hard-coded
        #rule based tests
        if added_reason == REJECTED:
            return "model rejection"

        #special reject for not recurrent direct deposit users and existing
        #users that their expiration is close
        if features[IS_RECURRING_DIRECT_DEPOSIT_IDX] == 0 and \
        features[IS_CLOSE_EXPIRATION_IDX] == 0:
            return "not recurrent and not close expiration"

        conditions = {
            "not active": lambda features: features[ACTIVE_USER_IDX] == 0,
            "not pro": lambda features: features[IS_PRO_CUSTOMER_IDX] == 0,
            "less than 30 days from signup": lambda features:
            features[TIME_FROM_SIGNUP_DATE_IDX] < MINIMUN_DAYS_FROM_SIGNUP,
            "money in <500": lambda features:
            features[TOTAL_MONEY_IN_IDX] <MINIMUM_MONEY_IN_FOR_OVERDRAFT
        }

        strout = []

        for condition_name, predicate in conditions.items():
            if (predicate(features)):
                strout.append(condition_name)

        return ' and '.join(strout)

    def post_prediction_filtration(self, features, prediction):
        if features[CURRENT_BALANCE_IDX] < 0 and \
                (predictions_to_overdraft[prediction] + features[CURRENT_BALANCE_IDX]) < \
                THRESHOLD_OF_DIFF_BETWEEN_NEGATIVE_BALANCE_AND_OVERDRAFT:
            return 0
        return prediction

    def remove_features_for_rejection(self, features):
        upd_feat = features
        for i in range(0, len(rejection_model_list)):
            upd_feat = np.delete(upd_feat, np.s_[rejection_model_list[i]], 1)

        return upd_feat

    def remove_extra_features(self, features):
        upd_feat = features
        for i in range(0, len(extra_features_removal_list)):
            upd_feat = np.delete(upd_feat, np.s_[extra_features_removal_list[i]], 1)

        return upd_feat


    def predict_proba(self, config, features):
        feat_in = self.remove_extra_features(features)
        return self.model_money_in.predict_proba(feat_in)

    def is_keep_old_balance(self, feature, prediction):
        # close expiration and not recurring, keep the old behaviour
        if feature[IS_CLOSE_EXPIRATION_IDX] == 1 and feature[IS_RECURRING_DIRECT_DEPOSIT_IDX] == 0:
            return True

        # in this case, we do a regular prediction and only accept it if the prediction is better, otherwise
        # we keep the old one
        if feature[IS_CLOSE_EXPIRATION_IDX] == 1 and feature[IS_RECURRING_DIRECT_DEPOSIT_IDX] == 1:
            if predictions_to_overdraft[prediction] < feature[OVERDRAFT_LIMIT_IDX]:
                return True

    def rejection_predict(self, features):
        rej_preds = []
        preds_proba = self.model_reject.predict_proba(features)
        for i in range(0, len(preds_proba)):
            if preds_proba[i][1] >= REJECTION_PREDICTION_THRESHOLD:
                rej_preds.append(1)
            else:
                rej_preds.append(0)

        return rej_preds