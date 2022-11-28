package org.apache.dolphinscheduler.dao.dto.alert;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.enums.AlertType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskResultAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String taskName;

    private String title;

    private ArrayNode result;

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_RESULT;
    }

}
