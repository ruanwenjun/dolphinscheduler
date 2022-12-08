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
public class TaskSuccessAlert implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String taskName;

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_SUCCESS;
    }
}
