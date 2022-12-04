package org.apache.dolphinscheduler.alert.sender;

import com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.alert.AlertPluginManager;
import org.apache.dolphinscheduler.alert.AlertServerMetrics;
import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.alert.content.AlertContentWrapper;
import org.apache.dolphinscheduler.alert.content.AlertContentWrapperGenerator;
import org.apache.dolphinscheduler.alert.filter.AlertFilter;
import org.apache.dolphinscheduler.common.enums.AlertStatus;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.dto.AlertPluginInstanceDTO;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.AlertSendStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DefaultAlertSender implements AlertSender {

    @Autowired
    private AlertDao alertDao;

    @Autowired
    private AlertPluginManager alertPluginManager;

    @Autowired
    private AlertContentWrapperGenerator alertContentAdaptorGenerator;

    @Autowired
    private List<AlertFilter> alertFilters;

    @Override
    public void sendAlert(List<Alert> alertList) {
        if (CollectionUtils.isEmpty(alertList)) {
            return;
        }
        for (Alert alert : alertList) {
            try {
                doSendSingleAlert(alert);
            } catch (Exception exception) {
                List<AlertResult> alertResults = Lists.newArrayList(new AlertResult(false, exception.getMessage()));
                alertDao.updateAlert(AlertStatus.EXECUTION_FAILURE, JSONUtils.toJsonString(alertResults),
                        alert.getId());
                AlertServerMetrics.incAlertFailCount();
            }
        }
    }

    private void doSendSingleAlert(@NonNull Alert alert) {
        List<AlertPluginInstanceDTO> alertPluginInstances = getAlertPluginInstance(alert);
        if (CollectionUtils.isEmpty(alertPluginInstances)) {
            throw new RuntimeException("The alert has no matched plugin instance");
        }
        List<AlertSendStatus> alertSendStatuses = new ArrayList<>();
        List<AlertResult> alertResults = new ArrayList<>();

        for (AlertPluginInstanceDTO instance : alertPluginInstances) {

            AlertResult alertResult = sendAlertByPluginInstance(instance, alert);
            AlertStatus sendStatus =
                    alertResult.isSuccess() ? AlertStatus.EXECUTION_SUCCESS : AlertStatus.EXECUTION_FAILURE;
            AlertSendStatus alertSendStatus = AlertSendStatus.builder()
                    .alertId(alert.getId())
                    .alertPluginInstanceId(instance.getId())
                    .sendStatus(sendStatus)
                    .log(JSONUtils.toJsonString(alertResult))
                    .createTime(new Date())
                    .build();
            alertSendStatuses.add(alertSendStatus);
            if (AlertStatus.EXECUTION_SUCCESS.equals(sendStatus)) {
                AlertServerMetrics.incAlertSuccessCount();
            } else {
                AlertServerMetrics.incAlertFailCount();
            }
            alertResults.add(alertResult);
        }
        // we update the alert first to avoid duplicate key in alertSendStatus
        // this may loss the alertSendStatus if the server restart
        // todo: use transaction to update these two table
        alertDao.updateAlert(getAlertStatus(alertResults, alertPluginInstances), JSONUtils.toJsonString(alertResults),
                alert.getId());
        alertDao.insertAlertSendStatus(alertSendStatuses);
    }

    private List<AlertPluginInstanceDTO> getAlertPluginInstance(@NonNull Alert alert) {
        int alertGroupId = Optional.ofNullable(alert.getAlertGroupId()).orElse(0);
        List<AlertPluginInstanceDTO> alertPluginInstances = alertDao.listInstanceDTOByAlertGroupId(alertGroupId);

        if (CollectionUtils.isEmpty(alertPluginInstances)) {
            throw new RuntimeException("No bind plugin instance");
        }

        if (CollectionUtils.isEmpty(alertFilters)) {
            return alertPluginInstances;
        }

        return alertPluginInstances.stream()
                .filter(alertPluginInstance -> {
                    for (AlertFilter alertFilter : alertFilters) {
                        if (!alertFilter.filter(alertPluginInstance, alert)) {
                            return false;
                        }
                    }
                    return true;
                }).collect(Collectors.toList());
    }

    private @NonNull AlertResult sendAlertByPluginInstance(AlertPluginInstanceDTO instance, Alert alert) {
        try {
            AlertChannel alertChannel = getAlertChannel(instance);
            Map<String, String> paramsMap = instance.getPluginInstanceParams();

            AlertContentWrapper alertContentAdaptor = alertContentAdaptorGenerator.generateAlertContent(alert);
            if (alertContentAdaptor == null) {
                throw new RuntimeException("Alert content is invalidated");
            }
            AlertData alertData = AlertData.builder()
                    .id(alert.getId())
                    .projectName(alertContentAdaptor.getAlertContentPojo().getProjectName())
                    .title(alertContentAdaptor.getAlertTitle())
                    .content(alertContentAdaptor.getAlertContent())
                    .build();

            AlertInfo alertInfo = AlertInfo.builder()
                    .alertData(alertData)
                    .alertParams(paramsMap)
                    .alertPluginInstanceId(instance.getId())
                    .build();
            AlertResult alertResult;
            if (alert.getAlertType() == AlertType.CLOSE_ALERT) {
                alertResult = alertChannel.closeAlert(alertInfo);
            } else {
                alertResult = alertChannel.process(alertInfo);
            }
            if (alertResult == null) {
                throw new RuntimeException("Alert result cannot be null");
            }
            return alertResult;
        } catch (Exception e) {
            log.error("send alert error alert data id :{},", alert.getId(), e);
            return AlertResult.error("Handle alert error, meet an unknown exception: " + e.getMessage());
        }
    }

    private AlertStatus getAlertStatus(@NonNull List<AlertResult> alertResults,
                                       @NonNull List<AlertPluginInstanceDTO> alertPluginInstances) {
        long successCount = alertResults.stream()
                .filter(AlertResult::isSuccess)
                .count();
        if (successCount == alertPluginInstances.size()) {
            return AlertStatus.EXECUTION_SUCCESS;
        }
        if (successCount == 0) {
            return AlertStatus.EXECUTION_FAILURE;
        }
        return AlertStatus.EXECUTION_PARTIAL_SUCCESS;
    }

    private AlertChannel getAlertChannel(AlertPluginInstanceDTO instance) {
        String pluginInstanceName = instance.getInstanceName();
        int pluginDefineId = instance.getPluginDefineId();
        Optional<AlertChannel> alertChannelOptional = alertPluginManager.getAlertChannel(instance.getPluginDefineId());
        return alertChannelOptional.orElseThrow(
                () -> new RuntimeException(
                        String.format("Alert Plugin %s send error: the channel doesn't exist, pluginDefineId: %s",
                                pluginInstanceName,
                                pluginDefineId)));
    }
}
