package org.apache.dolphinscheduler.alert.api.content;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowToleranceAlert implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String workflowInstanceHost;
    private String label;
    private String workflowInstanceLink;
    private Date alertCreateTime;

    @Override
    public AlertType getAlertType() {
        return AlertType.WORKFLOW_FAULT_TOLERANCE;
    }

}