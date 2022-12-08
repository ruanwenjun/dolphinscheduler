package org.apache.dolphinscheduler.alert.content;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "false", matchIfMissing = true)
public class DefaultAlertContentWrapperGenerator implements AlertContentWrapperGenerator {

    @Override
    public DefaultAlertContentWrapper generateAlertContent(@NonNull AlertContent alertContent) {
        return new DefaultAlertContentWrapper(alertContent);
    }
}
