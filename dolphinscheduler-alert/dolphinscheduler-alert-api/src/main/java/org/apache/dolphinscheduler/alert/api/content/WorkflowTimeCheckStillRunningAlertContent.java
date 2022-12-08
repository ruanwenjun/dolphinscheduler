package org.apache.dolphinscheduler.alert.api.content;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowTimeCheckStillRunningAlertContent implements AlertContent {

    private String projectName;
    private String workflowName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT;
    }

    @Override
    public String getProjectName() {
        return projectName;
    }

    @Override
    public String getWorkflowInstanceName() {
        return workflowInstanceName;
    }
}
