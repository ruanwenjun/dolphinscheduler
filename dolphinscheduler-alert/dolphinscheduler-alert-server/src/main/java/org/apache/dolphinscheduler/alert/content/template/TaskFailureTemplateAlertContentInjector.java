package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.TaskFailureAlertContent;
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
public class TaskFailureTemplateAlertContentInjector extends BaseAlertTemplateInjector {

    public TaskFailureTemplateAlertContentInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        TaskFailureAlertContent taskFailureAlertContent = (TaskFailureAlertContent) alertContent;

        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskFailureAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskFailureAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskFailureAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskFailureAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskFailureAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskFailureAlertContent.getWorkflowInstanceLink())
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskFailureAlertContent.getLabel());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskFailureAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskFailureAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskFailureAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskFailureAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskFailureAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskFailureAlertContent.getWorkflowInstanceLink())
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskFailureAlertContent.getLabel());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.TASK_FAILURE;
    }
}
