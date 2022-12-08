package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.alert.api.content.AlertContent;

public interface AlertTemplateInjector {

    @NonNull
    TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent);

    @NonNull
    AlertType getAlertType();
}
