package org.apache.dolphinscheduler.alert.content;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.alert.content.template.AlertTemplateInjector;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true", matchIfMissing = true)
public class TemplateAlertContentWrapperGenerator implements AlertContentWrapperGenerator {

    private Map<AlertType, AlertTemplateInjector> alertTemplateInjectorMap = new HashMap<>();

    public TemplateAlertContentWrapperGenerator(List<AlertTemplateInjector> alertTemplateInjectors) {
        if (CollectionUtils.isEmpty(alertTemplateInjectors)) {
            return;
        }
        alertTemplateInjectors.forEach(alertTemplateInjector -> {
            alertTemplateInjectorMap.put(alertTemplateInjector.getAlertType(), alertTemplateInjector);
        });
    }

    @Override
    public TemplateInjectedAlertContentWrapper generateAlertContent(@NonNull Alert alert) {
        AlertType alertType = alert.getAlertType();

        AlertTemplateInjector alertTemplateInjector = alertTemplateInjectorMap.get(alertType);
        AlertContent alertContent = JSONUtils.parseObject(alert.getContent(), AlertContent.class);
        return alertTemplateInjector.injectIntoTemplate(alertContent);
    }
}
