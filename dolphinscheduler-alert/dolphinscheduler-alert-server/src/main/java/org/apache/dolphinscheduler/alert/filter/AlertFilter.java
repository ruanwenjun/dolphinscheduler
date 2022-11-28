package org.apache.dolphinscheduler.alert.filter;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.dto.AlertPluginInstanceDTO;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.AlertPluginInstance;

/**
 * Used to filter the alert, only the alert match all filter can be send by the plugin instance.
 */
public interface AlertFilter {

    boolean filter(@NonNull AlertPluginInstanceDTO alertPluginInstance, @NonNull Alert alert);
}
