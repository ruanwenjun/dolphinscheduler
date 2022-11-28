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
public class TaskInstanceTimeoutAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String taskName;

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_TIMEOUT;
    }
}
