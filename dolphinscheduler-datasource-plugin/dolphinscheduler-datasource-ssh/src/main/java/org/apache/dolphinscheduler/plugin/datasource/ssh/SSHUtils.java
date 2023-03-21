package org.apache.dolphinscheduler.plugin.datasource.ssh;

import org.apache.dolphinscheduler.plugin.datasource.ssh.param.SSHConnectionParam;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.loader.KeyPairResourceLoader;
import org.apache.sshd.common.util.security.SecurityUtils;

import java.security.KeyPair;
import java.util.Collection;

public class SSHUtils {

    public static ClientSession getSession(SshClient client, SSHConnectionParam connectionParam) throws Exception {
        ClientSession session;
        session = client.connect(connectionParam.getUser(), connectionParam.getHost(), connectionParam.getPort())
                .verify(5000).getSession();
        // add password identity
        String password = connectionParam.getPassword();
        if (StringUtils.isNotEmpty(password)) {
            session.addPasswordIdentity(password);
        }

        // add public key identity
        String publicKey = connectionParam.getPublicKey();
        if (StringUtils.isNotEmpty(publicKey)) {
            try {
                KeyPairResourceLoader loader = SecurityUtils.getKeyPairResourceParser();
                Collection<KeyPair> keyPairCollection = loader.loadKeyPairs(null, null, null, publicKey);
                for (KeyPair keyPair : keyPairCollection) {
                    session.addPublicKeyIdentity(keyPair);
                }
            } catch (Exception e) {
                throw new Exception("Failed to add public key identity", e);
            }
        }
        return session;
    }
}
