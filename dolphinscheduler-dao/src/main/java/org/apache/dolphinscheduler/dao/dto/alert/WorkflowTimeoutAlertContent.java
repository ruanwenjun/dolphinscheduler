package org.apache.dolphinscheduler.dao.dto.alert;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.enums.AlertType;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowTimeoutAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_TIMEOUT;
    }

}
