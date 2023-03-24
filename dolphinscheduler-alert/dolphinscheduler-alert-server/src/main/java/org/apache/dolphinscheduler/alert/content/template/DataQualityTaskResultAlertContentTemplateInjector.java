package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.DqExecuteResultAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.alert.utils.AlertContentUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.DateUtils;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class DataQualityTaskResultAlertContentTemplateInjector extends BaseAlertTemplateInjector {

    public DataQualityTaskResultAlertContentTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        DqExecuteResultAlertContent dqExecuteResultAlertContent = (DqExecuteResultAlertContent) alertContent;

        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(dqExecuteResultAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, dqExecuteResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        dqExecuteResultAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(dqExecuteResultAlertContent.getAlertCreateTime()));

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(dqExecuteResultAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, dqExecuteResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        dqExecuteResultAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.RESULT_TEMPLATE, JSONUtils.writeAsPrettyString(alertContent))
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(dqExecuteResultAlertContent.getAlertCreateTime()));

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.DATA_QUALITY_TASK_RESULT;
    }
}