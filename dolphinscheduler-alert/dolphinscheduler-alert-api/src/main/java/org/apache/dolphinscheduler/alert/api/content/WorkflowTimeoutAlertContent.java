package org.apache.dolphinscheduler.alert.api.content;

import org.apache.dolphinscheduler.alert.api.enums.AlertType;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowTimeoutAlertContent implements AlertContent {

    private String projectName;
    private String workflowInstanceName;
    private Date alertCreateTime;
    private String label;
    private String workflowInstanceLink;
    private Date startTime;
    private Date endTime;

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_TIMEOUT;
    }

}
