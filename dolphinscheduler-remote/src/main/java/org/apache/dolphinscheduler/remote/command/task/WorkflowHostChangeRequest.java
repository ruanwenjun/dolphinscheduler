package org.apache.dolphinscheduler.remote.command.task;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowHostChangeRequest implements Serializable {

    private int taskInstanceId;

    private String workflowHost;

    /**
     * package request command
     *
     * @return command
     */
    public Command convert2Command() {
        Command command = new Command();
        command.setType(CommandType.WORKFLOW_HOST_CHANGE_REQUEST);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }
}
