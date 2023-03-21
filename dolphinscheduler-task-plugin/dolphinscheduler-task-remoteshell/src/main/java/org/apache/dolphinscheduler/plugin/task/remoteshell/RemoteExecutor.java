package org.apache.dolphinscheduler.plugin.task.remoteshell;

import org.apache.dolphinscheduler.plugin.datasource.ssh.SSHUtils;
import org.apache.dolphinscheduler.plugin.datasource.ssh.param.SSHConnectionParam;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.apache.sshd.sftp.client.fs.SftpFileSystem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteExecutor {
    protected final Logger logger =
        LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, getClass()));

    protected static final Pattern SETVALUE_REGEX = Pattern.compile(TaskConstants.SETVALUE_REGEX);

    final static String REMOTE_SHELL_HOME = "/tmp/dolphinscheduler-remote-shell-%s/";
    final static String STATUS_TAG_MESSAGE = "DOLPHINSCHEDULER-REMOTE-SHELL-TASK-STATUS-";

    final static int TRACK_INTERVAL = 5000;

    protected StringBuilder varPool = new StringBuilder();

    SshClient sshClient;
    ClientSession session;
    SSHConnectionParam sshConnectionParam;

    public RemoteExecutor(SSHConnectionParam sshConnectionParam) {

        this.sshConnectionParam = sshConnectionParam;
        initClient();
    }

    private void initClient() {
        sshClient = SshClient.setUpDefaultClient();
        sshClient.start();
    }

    private ClientSession getSession() {
        if (session != null && session.isOpen()) {
            return session;
        }
        try {
            session = SSHUtils.getSession(sshClient, sshConnectionParam);
            if (session == null || !session.auth().verify().isSuccess()) {
                throw new TaskException("SSH connection failed");
            }
        } catch (Exception e) {
            throw new TaskException("SSH connection failed", e);
        }
        return session;
    }

    public int run(String task_id, String localFile) throws IOException {
        try {
            // only run task if no exist same task
            String pid = getTaskPid(task_id);
            if (StringUtils.isEmpty(pid)) {
                saveCommand(task_id, localFile);
                String runCommand = String.format(COMMAND.RUN_COMMAND, getRemoteShellHome(), task_id, getRemoteShellHome(), task_id);
                runRemote(runCommand);
            }
            track(task_id);
            return getTaskExitCode(task_id);
        } catch (Exception e) {
            throw new TaskException("Remote shell task error", e);
        }
    }

    public void track(String task_id) throws Exception {
        int logN = 0;
        String pid;
        logger.info("Remote shell task log:");
        do {
            pid = getTaskPid(task_id);
            String trackCommand = String.format(COMMAND.TRACK_COMMAND, logN + 1, getRemoteShellHome(), task_id);
            String log = runRemote(trackCommand);
            if (StringUtils.isEmpty(log)) {
                Thread.sleep(TRACK_INTERVAL);
            }else {
                logN += log.split("\n").length;
                setVarPool(log);
                logger.info(log);
            }
        } while (StringUtils.isNotEmpty(pid));
    }

    public String getVarPool() {
        return varPool.toString();
    }

    private void setVarPool(String log) {
        String[] lines = log.split("\n");
        for (String line : lines) {
            if (line.startsWith("${setValue(") || line.startsWith("#{setValue(")) {
                varPool.append(findVarPool(line));
                varPool.append("$VarPool$");
            }
        }
    }

    private String findVarPool(String line) {
        Matcher matcher = SETVALUE_REGEX.matcher(line);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }



    public Integer getTaskExitCode(String task_id) throws IOException {
        String trackCommand = String.format(COMMAND.LOG_TAIL_COMMAND, getRemoteShellHome(), task_id);
        String log = runRemote(trackCommand);
        int exitCode = -1;
        logger.info("Remote shell task run status: {}", log);
        if (log.contains(STATUS_TAG_MESSAGE)) {
            String status = log.replace(STATUS_TAG_MESSAGE, "").trim();
            if (status.equals("0")) {
                logger.info("Remote shell task success");
                exitCode = 0;
            } else {
                logger.error("Remote shell task failed");
                exitCode = Integer.parseInt(status);
            }
        }
        cleanData(task_id);
        logger.error("Remote shell task failed");
        return exitCode;
    }

    public void cleanData(String task_id) {
        String cleanCommand = String.format(COMMAND.CLEAN_COMMAND, getRemoteShellHome(), task_id, getRemoteShellHome(), task_id);
        try {
            runRemote(cleanCommand);
        } catch (Exception e) {
            logger.error("Remote shell task clean data failed, but will not affect the task execution", e);
        }
    }

    public void kill(String task_id) throws IOException {
        String pid = getTaskPid(task_id);
        String killCommand = String.format(COMMAND.KILL_COMMAND, pid);
        runRemote(killCommand);
        cleanData(task_id);
    }

    public String getTaskPid(String task_id) throws IOException {
        String pid_command = String.format(COMMAND.GET_PID_COMMAND, task_id);
        return runRemote(pid_command).trim();
    }

    public void saveCommand(String task_id, String localFile) throws IOException {
        String checkDirCommand = String.format(COMMAND.CHECK_DIR, getRemoteShellHome(), getRemoteShellHome());
        runRemote(checkDirCommand);
        uploadScript(task_id, localFile);

        logger.info("The final script is: \n{}", runRemote(String.format(COMMAND.CAT_FINAL_SCRIPT, getRemoteShellHome(), task_id)));
    }

    public void uploadScript(String task_id, String localFile) throws IOException {

        String remotePath = getRemoteShellHome() + task_id + ".sh";
        logger.info("upload script from local:{} to remote: {}", localFile, remotePath);
        try(SftpFileSystem fs = SftpClientFactory.instance().createSftpFileSystem(getSession())) {
            Path path = fs.getPath(remotePath);
            Files.copy(Paths.get(localFile), path);
        }
    }


    public String runRemote(String command) throws IOException {
        ChannelExec channel = getSession().createExecChannel(command);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        channel.setOut(System.out);
        channel.setOut(out);
        channel.setErr(err);
        channel.open();
        channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), 0);
        channel.close();
        if (channel.getExitStatus() != 0) {
            throw new TaskException("Remote shell task error, error message: " + err.toString());
        }
        return out.toString();
    }

    private String getRemoteShellHome() {
        return String.format(REMOTE_SHELL_HOME, sshConnectionParam.getUser());
    }

    static class COMMAND {
        static final String CHECK_DIR = "if [ ! -d %s ]; then mkdir -p %s; fi";
        static final String RUN_COMMAND = "nohup /bin/bash %s%s.sh >%s%s.log 2>&1 &";
        static final String TRACK_COMMAND = "tail -n +%s %s%s.log";

        static final String LOG_TAIL_COMMAND = "tail -n 1 %s%s.log";
        static final String GET_PID_COMMAND = "ps -ef | grep \"%s.sh\" | grep -v grep | awk '{print $2}'";
        static final String KILL_COMMAND = "kill -9 %s";
        static final String CLEAN_COMMAND = "rm %s%s.sh %s%s.log";

        static final String HEADER = "#!/bin/bash\n";

        static final String ADD_STATUS_COMMAND = "\necho %s$?";

        static final String CAT_FINAL_SCRIPT = "cat %s%s.sh";
    };

}
