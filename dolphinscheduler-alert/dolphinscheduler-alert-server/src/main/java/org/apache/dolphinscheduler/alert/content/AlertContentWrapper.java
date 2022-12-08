package org.apache.dolphinscheduler.alert.content;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;

public interface AlertContentWrapper {

    String getAlertTitle();

    String getAlertContent();

    AlertContent getAlertContentPojo();
}
