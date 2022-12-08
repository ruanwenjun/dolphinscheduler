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
public class CloseAlertContent implements AlertContent {

    private String projectName;

    private String workflowInstanceName;

    @Override
    public String getProjectName() {
        return projectName;
    }

    @Override
    public String getWorkflowInstanceName() {
        return workflowInstanceName;
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.CLOSE_ALERT;
    }

}
