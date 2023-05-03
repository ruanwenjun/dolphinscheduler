package org.apache.dolphinscheduler.plugin.task.shell.executor.local;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.ShellCommandExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParameterUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.OSUtils;
import org.apache.dolphinscheduler.plugin.task.shell.ShellParameters;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecuteResult;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecutor;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalShellExecutor extends AbstractTaskExecutor implements ShellExecutor {

    protected final Logger logger = LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, getClass()));

    private final ShellCommandExecutor shellCommandExecutor;
    private final ShellParameters shellParameters;

    public LocalShellExecutor(TaskExecutionContext taskExecutionContext, ShellParameters shellParameters) {
        super(taskExecutionContext);
        this.shellCommandExecutor = new ShellCommandExecutor(this::logHandle, taskExecutionContext, logger);
        this.shellParameters = shellParameters;
    }

    @Override
    public ShellExecuteResult execute() {
        try {
            final String shellScriptFilePath = generateShellScriptFile();
            final TaskResponse taskResponse = shellCommandExecutor.run(shellScriptFilePath);
            return ShellExecuteResult.builder()
                .exitStatusCode(taskResponse.getExitStatusCode())
                .appIds(taskResponse.getAppIds())
                .processId(taskResponse.getProcessId())
                .varPool(taskResponse.getVarPool())
                .build();
        } catch (Exception ex) {
            throw new RuntimeException("Execute local shell task failed", ex);
        }
    }

    @Override
    public void handle() throws TaskException {
        throw new UnsupportedOperationException("Remote shell task doesn't support handle");
    }

    @Override
    public String getVarPool() {
        return shellCommandExecutor.getVarPool();
    }

    @Override
    public AbstractParameters getParameters() {
        return shellParameters;
    }

    @Override
    public void cancelTask() {
        try {
            shellCommandExecutor.cancelApplication();
        } catch (Exception e) {
            throw new RuntimeException("Cancel task failed", e);
        }
    }

    private String generateShellScriptFile() throws IOException {
        String shellFileAbsolutePath = String.format("%s/%s_node.%s", taskRequest.getExecutePath(),
            taskRequest.getTaskAppId(), OSUtils.isWindows() ? "bat" : "sh");

        Path path = Paths.get(shellFileAbsolutePath);

        if (Files.exists(path)) {
            throw new FileAlreadyExistsException(shellFileAbsolutePath);
        }

        injectParameter();
        log.info("Inject parameter successfully: \n{}", shellParameters.getRawScript());

        FileUtils.createFileWith755(path);
        Files.write(path, shellParameters.getRawScript().getBytes(), StandardOpenOption.APPEND);
        log.info("Create shell file : {} successfully", shellFileAbsolutePath);
        return shellFileAbsolutePath;
    }

    private void injectParameter() {
        String script = shellParameters.getRawScript().replaceAll("\\r\\n", "\n");
        // combining local and global parameters
        Map<String, Property> paramsMap = taskRequest.getPrepareParamsMap();
        script = ParameterUtils.convertParameterPlaceholders(script, ParamUtils.convert(paramsMap));
        shellParameters.setRawScript(script);
    }

}
