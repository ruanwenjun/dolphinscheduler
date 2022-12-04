package org.apache.dolphinscheduler.alert.content;

import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;

public interface AlertContentWrapper {

    String getAlertTitle();

    String getAlertContent();

    AlertContent getAlertContentPojo();
}
