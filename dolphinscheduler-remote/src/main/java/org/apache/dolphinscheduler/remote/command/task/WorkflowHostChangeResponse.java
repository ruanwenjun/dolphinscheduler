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
public class WorkflowHostChangeResponse implements Serializable {

    boolean success;

    public static WorkflowHostChangeResponse success() {
        WorkflowHostChangeResponse response = new WorkflowHostChangeResponse();
        response.setSuccess(true);
        return response;
    }

    public static WorkflowHostChangeResponse failed() {
        WorkflowHostChangeResponse response = new WorkflowHostChangeResponse();
        response.setSuccess(false);
        return response;
    }

    public Command convert2Command(long opaque) {
        Command command = new Command(opaque);
        command.setType(CommandType.WORKFLOW_HOST_CHANGE_RESPONSE);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }
}
