package org.apache.dolphinscheduler.alert.filter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.dolphinscheduler.alert.api.AlertConstants;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.dao.dto.AlertPluginInstanceDTO;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class PluginInstanceWarningTypeFilter implements AlertFilter {

    @Override
    public boolean filter(AlertPluginInstanceDTO alertPluginInstance, Alert alert) {
        Map<String, String> pluginInstanceParams = alertPluginInstance.getPluginInstanceParams();
        if (MapUtils.isEmpty(pluginInstanceParams)) {
            return true;
        }
        String instanceWarnType =
                pluginInstanceParams.getOrDefault(AlertConstants.NAME_WARNING_TYPE, WarningType.ALL.getDescp());
        if (WarningType.ALL.getDescp().equals(instanceWarnType)) {
            return true;
        }
        if (instanceWarnType.equals(alert.getWarningType().getDescp())) {
            return true;
        }
        log.warn(
                "The current alertPlugin instance has been filtered due to the warning type is not matched, pluginInstanceWarningType: {}, alertWarningType: {}",
                instanceWarnType, alert.getWarningType().getDescp());
        return false;
    }
}
