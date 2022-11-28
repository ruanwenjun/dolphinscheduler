package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.AlertPluginInstance;

import java.util.Date;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AlertPluginInstanceDTO {

    private int id;

    private int pluginDefineId;

    private String instanceName;

    private Map<String, String> pluginInstanceParams;

    private Date createTime;

    private Date updateTime;

    public AlertPluginInstanceDTO(@NonNull AlertPluginInstance alertPluginInstance) {
        this.id = alertPluginInstance.getId();
        this.pluginDefineId = alertPluginInstance.getPluginDefineId();
        this.instanceName = alertPluginInstance.getInstanceName();
        this.pluginInstanceParams = JSONUtils.toMap(alertPluginInstance.getPluginInstanceParams());
        this.createTime = alertPluginInstance.getCreateTime();
        this.updateTime = alertPluginInstance.getUpdateTime();
    }
}
