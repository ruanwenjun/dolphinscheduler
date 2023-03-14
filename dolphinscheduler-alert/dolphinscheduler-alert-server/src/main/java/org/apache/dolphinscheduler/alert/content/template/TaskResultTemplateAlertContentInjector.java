package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.TaskResultAlertContent;
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
public class TaskResultTemplateAlertContentInjector extends BaseAlertTemplateInjector {

    public TaskResultTemplateAlertContentInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        TaskResultAlertContent taskResultAlertContent = (TaskResultAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskResultAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.TITLE_TEMPLATE, taskResultAlertContent.getTitle())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskResultAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskResultAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskResultAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskResultAlertContent.getLabel())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskResultAlertContent.getWorkflowInstanceLink());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(taskResultAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.TITLE_TEMPLATE, taskResultAlertContent.getTitle())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskResultAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskResultAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.RESULT_TEMPLATE,
                        JSONUtils.writeAsPrettyString(taskResultAlertContent.getResult()))
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(taskResultAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, taskResultAlertContent.getLabel())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        taskResultAlertContent.getWorkflowInstanceLink());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.TASK_RESULT;
    }
}
