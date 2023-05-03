package org.apache.dolphinscheduler.plugin.task.shell.executor;

import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.ResourceType;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.AbstractResourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.DataSourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.shell.ShellParameters;
import org.apache.dolphinscheduler.plugin.task.shell.executor.local.LocalShellExecutor;
import org.apache.dolphinscheduler.plugin.task.shell.executor.remote.RemoteShellExecutor;
import org.apache.dolphinscheduler.plugin.task.shell.executor.remote.SSHConnectionParam;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShellExecutorFactory {

    public static ShellExecutor createShellExecutor(ShellParameters shellParameters, TaskExecutionContext taskExecutionContext) {
        if (isRemoteMode(shellParameters)) {
            return createRemoteShellExecutor(shellParameters, taskExecutionContext);
        }
        return createLocalShellExecutor(shellParameters, taskExecutionContext);
    }

    private static boolean isRemoteMode(ShellParameters shellParameters) {
        return shellParameters.getRemoteConnection() != null;
    }

    private static LocalShellExecutor createLocalShellExecutor(ShellParameters shellParameters,
                                                               TaskExecutionContext taskExecutionContext) {
        return new LocalShellExecutor(taskExecutionContext, shellParameters);
    }

    private static RemoteShellExecutor createRemoteShellExecutor(ShellParameters shellParameters,
                                                                 TaskExecutionContext taskExecutionContext) {
        ShellParameters.RemoteConnection remoteConnection = shellParameters.getRemoteConnection();
        ResourceParametersHelper resourceParametersHelper = taskExecutionContext.getResourceParametersHelper();
        Map<Integer, AbstractResourceParameters> resourceMap = resourceParametersHelper.getResourceMap(ResourceType.DATASOURCE);
        DataSourceParameters dataSourceParameters = (DataSourceParameters) resourceMap.get(remoteConnection.getDatasourceId());
        SSHConnectionParam sshConnectionParam = generateSSHConnectionParam(dataSourceParameters);
        return new RemoteShellExecutor(shellParameters, sshConnectionParam, taskExecutionContext);
    }

    private static SSHConnectionParam generateSSHConnectionParam(DataSourceParameters dataSourceParameters) {

        SSHConnectionParam sshConnectionParam = (SSHConnectionParam) DataSourceUtils.buildConnectionParams(
            dataSourceParameters.getType(), dataSourceParameters.getConnectionParams());
        return SSHConnectionParam.builder()
            .host(sshConnectionParam.getHost())
            .port(sshConnectionParam.getPort())
            .username(sshConnectionParam.getUsername())
            .password(sshConnectionParam.getPassword())
            .publicKey(sshConnectionParam.getPublicKey())
            .build();
    }

}
