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
public class WorkflowFailureAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_FAILURE;
    }

    @Override
    public String getWorkflowInstanceName() {
        return workflowInstanceName;
    }

}
