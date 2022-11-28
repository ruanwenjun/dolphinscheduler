package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;

public interface AlertTemplateInjector {

    @NonNull
    TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent);

    @NonNull
    AlertType getAlertType();
}
