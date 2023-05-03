package org.apache.dolphinscheduler.plugin.task.shell.executor.remote;

import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.shell.executor.ShellExecuteResult;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.loader.KeyPairResourceLoader;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.apache.sshd.sftp.client.fs.SftpFileSystem;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.Collection;
import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SSHClient {

    public final static String REMOTE_SHELL_HOME = "/tmp/dolphinscheduler-remote-shell-%s/";
    public final static String STATUS_TAG_MESSAGE = "DOLPHINSCHEDULER-REMOTE-SHELL-TASK-STATUS-";
    private static final Pattern SETVALUE_REGEX = Pattern.compile(TaskConstants.SETVALUE_REGEX);
    private final static int TRACK_INTERVAL = 5000;
    private final StringBuilder varPool = new StringBuilder();
    private final SSHConnectionParam sshConnectionParam;
    private final SshClient sshClient;
    private ClientSession session;
    private final String appId;

    public SSHClient(String appId, SSHConnectionParam sshConnectionParam) {
        this.sshConnectionParam = sshConnectionParam;
        this.sshClient = createAndStartSshClient();
        this.session = generateNewSession();
        this.appId = appId;
    }

    public ShellExecuteResult executeShell(String shellScriptFile) {
        try {
            if (StringUtils.isEmpty(getTaskPid())) {
                uploadLocalFileToRemote(shellScriptFile);
                String runCommand =
                        String.format(COMMAND.RUN_COMMAND, getRemoteShellHome(), appId, getRemoteShellHome(), appId);
                runRemote(runCommand);
            }
            track();
            Integer taskExitCode = getTaskExitCode();
            return ShellExecuteResult.builder()
                    // Do we need to inject the remove process Id?
                    .appIds(appId)
                    .exitStatusCode(taskExitCode)
                    .varPool(varPool.toString())
                    .build();
        } catch (Exception ex) {
            throw new RuntimeException("Execute shell on remote failed", ex);
        }
    }

    public String getVarPool() {
        return varPool.toString();
    }

    public void cancel() {
        String pid;
        try {
            pid = getTaskPid();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get remote shell pid", e);
        }
        String killCommand = String.format(COMMAND.KILL_COMMAND, pid);
        try {
            runRemote(killCommand);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to kill remote shell", ex);
        }
        cleanRemoteData(appId);
    }

    private ClientSession getSession() {
        if (session != null) {
            // do we need to check if the session is alive?
            return session;
        }
        session = generateNewSession();
        return session;
    }

    public ClientSession generateNewSession() {
        ClientSession session;
        try {
            session = sshClient.connect(
                    sshConnectionParam.getUsername(),
                    sshConnectionParam.getHost(),
                    sshConnectionParam.getPort()).verify(10 * 1000).getSession();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create new session to remote server: " + sshConnectionParam.getHost(),
                    e);
        }
        String password = sshConnectionParam.getPassword();
        if (StringUtils.isNotEmpty(password)) {
            session.addPasswordIdentity(password);
        }

        String publicKey = sshConnectionParam.getPublicKey();
        if (StringUtils.isNotEmpty(publicKey)) {
            try {
                KeyPairResourceLoader loader = SecurityUtils.getKeyPairResourceParser();
                Collection<KeyPair> keyPairCollection = loader.loadKeyPairs(null, null, null, publicKey);
                for (KeyPair keyPair : keyPairCollection) {
                    session.addPublicKeyIdentity(keyPair);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to add public key identity", e);
            }
        }
        return session;
    }

    private SshClient createAndStartSshClient() {
        SshClient sshClient = SshClient.setUpDefaultClient();
        sshClient.start();
        return sshClient;
    }

    public String getTaskPid() throws IOException {
        String pidCommand = String.format(COMMAND.GET_PID_COMMAND, appId);
        return runRemote(pidCommand).trim();
    }

    private String runRemote(String shellCommand) throws IOException {
        ChannelExec channel = getSession().createExecChannel(shellCommand);
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

    public void uploadLocalFileToRemote(String localFile) throws IOException {
        String checkDirCommandExist = String.format(COMMAND.CHECK_DIR, getRemoteShellHome(), getRemoteShellHome());
        runRemote(checkDirCommandExist);
        doUpload(appId, localFile);
        log.info("The final script is: \n{}",
                runRemote(String.format(COMMAND.CAT_FINAL_SCRIPT, getRemoteShellHome(), appId)));
    }

    private void doUpload(String task_id, String localFile) throws IOException {

        String remotePath = getRemoteShellHome() + task_id + ".sh";
        log.info("upload script from local:{} to remote: {}", localFile, remotePath);
        try (SftpFileSystem fs = SftpClientFactory.instance().createSftpFileSystem(getSession())) {
            Path path = fs.getPath(remotePath);
            Files.copy(Paths.get(localFile), path);
        }
    }

    private String getRemoteShellHome() {
        return String.format(REMOTE_SHELL_HOME, sshConnectionParam.getUsername());
    }

    private void track() throws Exception {
        int logN = 0;
        String pid;
        log.info("Remote shell task log:");
        do {
            pid = getTaskPid();
            String trackCommand = String.format(COMMAND.TRACK_COMMAND, logN + 1, getRemoteShellHome(), appId);
            String taskLog = runRemote(trackCommand);
            if (StringUtils.isEmpty(taskLog)) {
                Thread.sleep(TRACK_INTERVAL);
            } else {
                logN += taskLog.split("\n").length;
                injectVarPool(taskLog);
                log.info(taskLog);
            }
        } while (StringUtils.isNotEmpty(pid));
    }

    private void injectVarPool(String log) {
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

    private Integer getTaskExitCode() throws IOException {
        String trackCommand = String.format(COMMAND.LOG_TAIL_COMMAND, getRemoteShellHome(), appId);
        String taskLog = runRemote(trackCommand);
        int exitCode = -1;
        log.info("Remote shell task run status: {}", log);
        if (taskLog.contains(STATUS_TAG_MESSAGE)) {
            String status = taskLog.replace(STATUS_TAG_MESSAGE, "").trim();
            if (status.equals("0")) {
                log.info("Remote shell task success");
                exitCode = 0;
            } else {
                log.error("Remote shell task failed");
                exitCode = Integer.parseInt(status);
            }
        }
        cleanRemoteData(appId);
        log.error("Remote shell task failed");
        return exitCode;
    }

    private void cleanRemoteData(String appId) {
        String cleanCommand =
                String.format(COMMAND.CLEAN_COMMAND, getRemoteShellHome(), appId, getRemoteShellHome(), appId);
        try {
            runRemote(cleanCommand);
        } catch (Exception e) {
            log.error("Remote shell task clean data failed, but will not affect the task execution", e);
        }
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
    }

}
