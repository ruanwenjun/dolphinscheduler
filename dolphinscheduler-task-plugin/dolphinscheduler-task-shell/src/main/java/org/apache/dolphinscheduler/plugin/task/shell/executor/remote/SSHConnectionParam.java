package org.apache.dolphinscheduler.plugin.task.shell.executor.remote;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SSHConnectionParam {

    private String username;
    private String password;
    private String publicKey;
    private String host;
    private int port;
}
