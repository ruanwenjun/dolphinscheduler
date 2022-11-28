package org.apache.dolphinscheduler.alert.content;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "false", matchIfMissing = true)
public class DefaultAlertContentWrapperGenerator implements AlertContentWrapperGenerator {

    @Override
    public DefaultAlertContentWrapper generateAlertContent(@NonNull Alert alert) {
        return new DefaultAlertContentWrapper(JSONUtils.parseObject(alert.getContent(), AlertContent.class));
    }
}
