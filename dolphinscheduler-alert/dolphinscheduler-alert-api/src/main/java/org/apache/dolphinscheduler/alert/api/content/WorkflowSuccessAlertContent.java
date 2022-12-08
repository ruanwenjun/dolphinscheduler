package org.apache.dolphinscheduler.alert.api.content;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowSuccessAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_SUCCESS;
    }

}
