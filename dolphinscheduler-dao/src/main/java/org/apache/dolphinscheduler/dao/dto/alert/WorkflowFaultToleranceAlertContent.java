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
public class WorkflowFaultToleranceAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.WORKFLOW_FAULT_TOLERANCE;
    }

}
