package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.WorkflowTimeoutAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.alert.utils.AlertContentUtils;
import org.apache.dolphinscheduler.spi.utils.DateUtils;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable")
public class WorkflowInstanceTimeoutAlertContentTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowInstanceTimeoutAlertContentTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowTimeoutAlertContent workflowTimeoutAlertContent = (WorkflowTimeoutAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeoutAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowTimeoutAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeoutAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeoutAlertContent.getAlertCreateTime()));

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeoutAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowTimeoutAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeoutAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeoutAlertContent.getAlertCreateTime()));

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_TIMEOUT;
    }
}
