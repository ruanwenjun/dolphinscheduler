package org.apache.dolphinscheduler.plugin.task.shell.executor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShellExecuteResult {

    private int exitStatusCode;
    private String appIds;
    private int processId;
    private String varPool;

}
