package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.TaskFaultToleranceAlertContent;
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
public class TaskFaultToleranceTemplateAlertContentInjector extends BaseAlertTemplateInjector {

    public TaskFaultToleranceTemplateAlertContentInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        TaskFaultToleranceAlertContent taskFaultToleranceAlertContent = (TaskFaultToleranceAlertContent) alertContent;

        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskFaultToleranceAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskFaultToleranceAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskFaultToleranceAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskFaultToleranceAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskFaultToleranceAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskFaultToleranceAlertContent.getWorkflowInstanceLink())
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskFaultToleranceAlertContent.getLabel());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskFaultToleranceAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskFaultToleranceAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskFaultToleranceAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskFaultToleranceAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskFaultToleranceAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskFaultToleranceAlertContent.getWorkflowInstanceLink())
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskFaultToleranceAlertContent.getLabel());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.TASK_FAULT_TOLERANCE;
    }
}
