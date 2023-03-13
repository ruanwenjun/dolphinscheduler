package org.apache.dolphinscheduler.alert.utils;

import org.apache.dolphinscheduler.alert.api.enums.AlertType;

public class AlertContentUtils {

    // todo: use config to judge the is8n
    public static String getAlertType(AlertType alertType) {
        return alertType.getDescpCN();
    }
}
