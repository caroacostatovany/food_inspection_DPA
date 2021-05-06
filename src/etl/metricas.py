from sklearn.metrics import precision_recall_curve, roc_curve, roc_auc_score


def get_metrics_report(fpr, tpr, thresholds, precision, recall, thresholds_2):
    """

    """
    df_1 = pd.DataFrame({'threshold': thresholds_2, 'precision': precision,
                         'recall': recall})
    df_1['f1_score'] = 2 * (df_1.precision * df_1.recall) / (df_1.precision + df_1.recall)

    df_2 = pd.DataFrame({'tpr': tpr, 'fpr': fpr, 'threshold': thresholds})
    df_2['tnr'] = 1 - df_2['fpr']
    df_2['fnr'] = 1 - df_2['tpr']

    df = df_1.merge(df_2, on="threshold")

    return df


def get_metrics_matrix(y_test, predicted_scores):
    """

    """
    fpr, tpr, thresholds = roc_curve(y_test, predicted_scores[:, 1], pos_label=1)
    precision, recall, thresholds_2 = precision_recall_curve(y_test, predicted_scores[:, 1], pos_label=1)
    thresholds_2 = np.append(thresholds_2, 1)

    metrics_report = get_metrics_report(fpr, tpr, thresholds, precision, recall, thresholds_2)

    return metrics_report
