package org.apache.dolphinscheduler.plugin.datasource.ssh.param;

import lombok.Data;

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.spi.enums.DbType;

@Data
public class SSHDataSourceParamDTO extends BaseDataSourceParamDTO {

    protected String publicKey;

    @Override
    public DbType getType() {
        return DbType.SSH;

    }
}
