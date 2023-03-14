package org.apache.dolphinscheduler.alert.api.content;

import com.fasterxml.jackson.databind.node.ArrayNode;
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
public class TaskResultAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private String taskName;
    private String label;
    private String workflowInstanceLink;
    private Date alertCreateTime;
    private Date startTime;
    private Date endTime;

    private String title;

    private ArrayNode result;

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_RESULT;
    }

}
