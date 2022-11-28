package org.apache.dolphinscheduler.dao.dto.alert;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.enums.AlertType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowTimeCheckNotRunAlertContent implements AlertContent {

    private String projectName;
    private String workflowName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_NOT_RUN_ALERT;
    }

    @Override
    public String getProjectName() {
        return projectName;
    }

    @Override
    public String getWorkflowInstanceName() {
        return workflowName;
    }
}
