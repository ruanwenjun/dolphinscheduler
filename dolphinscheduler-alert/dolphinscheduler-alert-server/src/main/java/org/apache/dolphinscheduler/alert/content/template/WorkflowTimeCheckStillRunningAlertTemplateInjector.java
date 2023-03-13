package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.WorkflowTimeCheckStillRunningAlertContent;
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
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class WorkflowTimeCheckStillRunningAlertTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowTimeCheckStillRunningAlertTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowTimeCheckStillRunningAlertContent workflowTimeCheckStillRunningAlertContent =
                (WorkflowTimeCheckStillRunningAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeCheckStillRunningAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeCheckStillRunningAlertContent.getAlertCreateTime()));

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeCheckStillRunningAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeCheckStillRunningAlertContent.getAlertCreateTime()));

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT;
    }
}
