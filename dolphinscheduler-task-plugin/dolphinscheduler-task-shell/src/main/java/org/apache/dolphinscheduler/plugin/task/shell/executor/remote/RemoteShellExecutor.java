package org.apache.dolphinscheduler.plugin.task.shell.executor.remote;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.OSUtils;
import org.apache.dolphinscheduler.plugin.task.shell.ShellParameters;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecuteResult;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecutor;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class RemoteShellExecutor extends AbstractTaskExecutor implements ShellExecutor {

    private final static String TASK_ID_PREFIX = "dolphinscheduler-remoteshell-";

    private final ShellParameters shellParameters;
    private final SSHClient sshClient;

    public RemoteShellExecutor(ShellParameters shellParameters,
                               SSHConnectionParam sshConnectionParam,
                               TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.shellParameters = shellParameters;
        if (taskExecutionContext.getAppIds() == null) {
            taskExecutionContext.setAppIds(TASK_ID_PREFIX + taskExecutionContext.getTaskInstanceId());
        }
        this.sshClient = new SSHClient(taskExecutionContext.getTaskAppId(), sshConnectionParam);
    }

    @Override
    public ShellExecuteResult execute() {
        try {
            String shellScriptFile = generateShellScriptFile();
            return sshClient.executeShell(shellScriptFile);
        } catch (Exception e) {
            throw new RuntimeException("Execute remote shell task failed", e);
        }
    }

    @Override
    public void handle() throws TaskException {
        throw new UnsupportedOperationException("Remote shell task doesn't support handle");
    }

    @Override
    public String getVarPool() {
        return sshClient.getVarPool();
    }

    @Override
    public AbstractParameters getParameters() {
        return shellParameters;
    }

    @Override
    public void cancelTask() {
        sshClient.cancel();
    }

    private String generateShellScriptFile() throws Exception {
        // generate scripts
        String shellFileAbsolutePath = String.format("%s/%s_node.%s",
            taskRequest.getExecutePath(),
            taskRequest.getTaskAppId(), OSUtils.isWindows() ? "bat" : "sh");

        File file = new File(shellFileAbsolutePath);
        Path path = file.toPath();

        if (Files.exists(path)) {
            // this shouldn't happen
            logger.warn("The script file: {} is already exist", path);
            return shellFileAbsolutePath;
        }
        injectParameter();
        injectEnvironment();
        injectReturnCode();
        FileUtils.createFileWith755(path);
        Files.write(path, shellParameters.getRawScript().getBytes(), StandardOpenOption.APPEND);
        logger.info("Create shell script : {} successfully", shellParameters.getRawScript());
        return shellFileAbsolutePath;
    }

    private void injectParameter() {
        String script = shellParameters.getRawScript().replaceAll("\\r\\n", "\n");
        // combining local and global parameters
        Map<String, Property> paramsMap = taskRequest.getPrepareParamsMap();
        script = ParameterUtils.convertParameterPlaceholders(script, ParamUtils.convert(paramsMap));
        shellParameters.setRawScript(script);
    }

    private void injectEnvironment() {
        String script = shellParameters.getRawScript();
        String environment = taskRequest.getEnvironmentConfig();
        if (environment != null) {
            environment = environment.replaceAll("\\r\\n", "\n");
            script = environment + "\n" + script;
        }
        shellParameters.setRawScript(script);
    }

    private void injectReturnCode() {
        String script = shellParameters.getRawScript();
        script = String.format(SSHClient.COMMAND.HEADER) + script;
        script += String.format(SSHClient.COMMAND.ADD_STATUS_COMMAND, SSHClient.STATUS_TAG_MESSAGE);
        shellParameters.setRawScript(script);
    }

}
