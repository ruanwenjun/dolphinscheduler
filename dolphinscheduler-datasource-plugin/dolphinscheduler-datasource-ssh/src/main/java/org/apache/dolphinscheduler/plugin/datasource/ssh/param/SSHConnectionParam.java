package org.apache.dolphinscheduler.plugin.datasource.ssh.param;

import lombok.Data;

import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;

import com.fasterxml.jackson.annotation.JsonInclude;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SSHConnectionParam implements ConnectionParam {

    protected String user;

    protected String password;

    protected String publicKey;

    protected String host;

    protected int port = 22;
}
