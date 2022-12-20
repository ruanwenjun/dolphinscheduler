package org.apache.dolphinscheduler.alert.api.content;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;

import java.util.Date;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(value = CloseAlertContent.class, name = "CLOSE_ALERT"),
        @JsonSubTypes.Type(value = TaskFailureAlertContent.class, name = "TASK_FAILURE"),
        @JsonSubTypes.Type(value = TaskFaultToleranceAlertContent.class, name = "TASK_FAULT_TOLERANCE"),
        @JsonSubTypes.Type(value = TaskInstanceTimeoutAlertContent.class, name = "TASK_TIMEOUT"),
        @JsonSubTypes.Type(value = TaskSuccessAlert.class, name = "TASK_SUCCESS"),
        @JsonSubTypes.Type(value = WorkflowBlockAlertContent.class, name = "PROCESS_INSTANCE_BLOCKED"),
        @JsonSubTypes.Type(value = WorkflowFailureAlertContent.class, name = "PROCESS_INSTANCE_FAILURE"),
        @JsonSubTypes.Type(value = WorkflowFaultToleranceAlertContent.class, name = "WORKFLOW_FAULT_TOLERANCE"),
        @JsonSubTypes.Type(value = WorkflowSuccessAlertContent.class, name = "PROCESS_INSTANCE_SUCCESS"),
        @JsonSubTypes.Type(value = WorkflowTimeoutAlertContent.class, name = "PROCESS_INSTANCE_TIMEOUT"),
        @JsonSubTypes.Type(value = WorkflowToleranceAlert.class, name = "FAULT_TOLERANCE_WARNING"),
        @JsonSubTypes.Type(value = TaskResultAlertContent.class, name = "TASK_RESULT"),
        @JsonSubTypes.Type(value = DqExecuteResultAlertContent.class, name = "DATA_QUALITY_RESULT"),
        @JsonSubTypes.Type(value = WorkflowTimeCheckNotRunAlertContent.class, name = "WORKFLOW_TIME_CHECK_NOT_RUN_ALERT"),
        @JsonSubTypes.Type(value = WorkflowTimeCheckStillRunningAlertContent.class, name = "WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT"),
        @JsonSubTypes.Type(value = ServerAlertContent.class, name = "SERVER_CRASH_ALERT"),
})
public interface AlertContent {

    AlertType getAlertType();

    String getProjectName();

    String getWorkflowInstanceName();

    default Date getStartTime() {
        return null;
    }

    default Date getEndTime() {
        return null;
    }

    default String getAlertTitle() {
        return String.format("[WhaleScheduler-%s] %s", getAlertType(), getProjectName());
    }

}
