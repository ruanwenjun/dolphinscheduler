package org.apache.dolphinscheduler.remote.command.alert;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.utils.JsonSerializer;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskAlertRequestCommand implements Serializable {

    private int workflowInstanceId;
    private String workflowInstanceName;
    private String taskName;
    private int groupId;
    private String title;
    private ArrayNode content;
    private WarningType warningType;

    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.TASK_RESULT_ALERT_SEND_REQUEST);
        byte[] body = JsonSerializer.serialize(this);
        command.setBody(body);
        return command;
    }
}
