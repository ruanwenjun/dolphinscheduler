package org.apache.dolphinscheduler.plugin.task.api.utils;

import org.apache.dolphinscheduler.plugin.task.api.SupplierWithIO;
import org.apache.dolphinscheduler.plugin.task.api.ThrowingSupplier;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import static org.apache.dolphinscheduler.spi.utils.Constants.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.dolphinscheduler.spi.utils.Constants.JAVA_SECURITY_KRB5_CONF;
import static org.apache.dolphinscheduler.spi.utils.Constants.JAVA_SECURITY_KRB5_CONF_PATH;
import static org.apache.dolphinscheduler.spi.utils.Constants.KERBEROS;
import static org.apache.dolphinscheduler.spi.utils.Constants.LOGIN_USER_KEY_TAB_PATH;
import static org.apache.dolphinscheduler.spi.utils.Constants.LOGIN_USER_KEY_TAB_USERNAME;

@Slf4j
public class KerberosUtils {

    private static final String AUTH_FAIL_MSG = "GSS initiate failed";
    private static final String INVALID_CREDENTIALS = "Invalid Credentials";
    private static final Object KERBEROS_LOCK = new Object();

    public static <T> T doWithReloginIfAuthFail(@NonNull SupplierWithIO<T> supplier,
                                                Configuration configuration) throws IOException {
        T result = null;
        try {
            result = supplier.get();
        } catch (IOException e) {
            if (e.getMessage() != null) {
                if (e.getMessage().contains(AUTH_FAIL_MSG) || e.getMessage().contains(INVALID_CREDENTIALS)) {
                    log.warn("Kerberos auth fail, will try to login again");
                    KerberosUtils.loadKerberosConf(configuration);
                    result = supplier.get();
                }
            }
        }
        return result;
    }

    public static <T> T doWithReloadKerberosConfIfAuthFail(@NonNull ThrowingSupplier<T> supplier,
                                                           String javaSecurityKrb5Conf,
                                                           String loginUserKeytabUsername,
                                                           String loginUserKeytabPath) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setClassLoader(configuration.getClass().getClassLoader());
        return doWithReloadKerberosConfIfAuthFail(supplier, javaSecurityKrb5Conf, loginUserKeytabUsername,
                loginUserKeytabPath, configuration);
    }

    public static <T> T doWithReloadKerberosConfIfAuthFail(@NonNull ThrowingSupplier<T> supplier,
                                                           String javaSecurityKrb5Conf,
                                                           String loginUserKeytabUsername,
                                                           String loginUserKeytabPath,
                                                           Configuration configuration) throws Exception {
        T result = null;
        try {
            result = supplier.get();
        } catch (IOException e) {
            if (e.getMessage() != null) {
                if (e.getMessage().contains(AUTH_FAIL_MSG) || e.getMessage().contains(INVALID_CREDENTIALS)) {
                    log.warn("Kerberos auth fail, will try to login again");
                    loadKerberosConf(javaSecurityKrb5Conf, loginUserKeytabUsername, loginUserKeytabPath, configuration);
                    result = supplier.get();
                }
            }
        }
        return result;
    }

    public static void checkTGTAndReloginFromKeytabWithLock() throws IOException {
        synchronized (KERBEROS_LOCK) {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        }
    }

    public static void loginUserFromKeytabWithLock(String user, String path) throws IOException {
        synchronized (KERBEROS_LOCK) {
            UserGroupInformation.loginUserFromKeytab(user, path);
        }
    }

    /**
     * load kerberos configuration
     *
     * @param configuration
     * @return load kerberos config return true
     * @throws IOException errors
     */
    public static void loadKerberosConf(Configuration configuration) throws IOException {
        loadKerberosConf(PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH),
                PropertyUtils.getString(LOGIN_USER_KEY_TAB_USERNAME),
                PropertyUtils.getString(LOGIN_USER_KEY_TAB_PATH), configuration);
    }

    /**
     * load kerberos configuration
     *
     * @param javaSecurityKrb5Conf javaSecurityKrb5Conf
     * @param loginUserKeytabUsername loginUserKeytabUsername
     * @param loginUserKeytabPath loginUserKeytabPath
     * @param configuration configuration
     * @return load kerberos config return true
     * @throws IOException errors
     */
    public static UserGroupInformation loadKerberosConf(String javaSecurityKrb5Conf, String loginUserKeytabUsername,
                                                        String loginUserKeytabPath,
                                                        Configuration configuration) throws IOException {

        System.setProperty(JAVA_SECURITY_KRB5_CONF, StringUtils.defaultIfBlank(javaSecurityKrb5Conf,
                PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH)));
        configuration.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS);
        UserGroupInformation.setConfiguration(configuration);

        loginUserFromKeytabWithLock(
                StringUtils.defaultIfBlank(loginUserKeytabUsername,
                        PropertyUtils.getString(LOGIN_USER_KEY_TAB_USERNAME)),
                StringUtils.defaultIfBlank(loginUserKeytabPath, PropertyUtils.getString(LOGIN_USER_KEY_TAB_PATH)));

        return UserGroupInformation.getCurrentUser();
    }
}
