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
public class WorkflowBlockAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private Date alertCreateTime;

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_BLOCKED;
    }

}
