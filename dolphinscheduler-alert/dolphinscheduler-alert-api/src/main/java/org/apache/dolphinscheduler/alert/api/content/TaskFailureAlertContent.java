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
public class TaskFailureAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String taskName;

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_FAILURE;
    }

    @Override
    public String getProjectName() {
        return projectName;
    }

}
