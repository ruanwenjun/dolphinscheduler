/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.common.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ResUploadType;
import org.apache.dolphinscheduler.common.exception.BaseException;
import org.apache.dolphinscheduler.common.exception.StorageOperateNoConfiguredException;
import org.apache.dolphinscheduler.common.storage.StorageOperate;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.utils.KerberosUtils;
import org.apache.dolphinscheduler.spi.enums.ResourceType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.dolphinscheduler.common.Constants.FOLDER_SEPARATOR;
import static org.apache.dolphinscheduler.common.Constants.FORMAT_S_S;
import static org.apache.dolphinscheduler.common.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.common.Constants.RESOURCE_TYPE_FILE;
import static org.apache.dolphinscheduler.common.Constants.RESOURCE_TYPE_UDF;

/**
 * hadoop utils
 * single instance
 */
public class HadoopUtils implements Closeable, StorageOperate {

    private static final Logger logger = LoggerFactory.getLogger(HadoopUtils.class);
    public static final int HADOOP_RESOURCE_MANAGER_HTTP_ADDRESS_PORT_VALUE =
        PropertyUtils.getInt(Constants.HADOOP_RESOURCE_MANAGER_HTTPADDRESS_PORT, 8088);

    private final String hdfsUser;
    private Configuration configuration;
    private FileSystem fs;

    /**
     * HadoopUtils single
     */
    private enum HDFSSingleton {

        INSTANCE;

        private final HadoopUtils instance;

        HDFSSingleton() {
            instance = new HadoopUtils();
        }

        private HadoopUtils getInstance() {
            return instance;
        }
    }

    public HadoopUtils() {
        logger.info("Init hdfs storage operator");
        hdfsUser = PropertyUtils.getString(Constants.HDFS_ROOT_USER);
        init();
    }

    public static HadoopUtils getInstance() {
        return HDFSSingleton.INSTANCE.getInstance();
    }

    /**
     * sync init
     */
    private synchronized void init() {
        initConfig();
        initHdfsPath();
    }

    /**
     * init dolphinscheduler root path in hdfs
     */

    private void initHdfsPath() {
        Path path = new Path(RESOURCE_UPLOAD_PATH);
        try {
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
        } catch (Exception e) {
            logger.error("Init HDFS path fail", e);
        }
    }

    /**
     * init HDFS configuration
     */
    private void initConfig() {
        logger.info("Start to init HDFS config");
        try {
            configuration = new HdfsConfiguration();
            if (CommonUtils.getKerberosStartupState()) {
                String defaultFS = configuration.get(Constants.FS_DEFAULT_FS);
                // first get key from core-site.xml hdfs-site.xml ,if null ,then try to get from properties file
                // the default is the local file system
                if (defaultFS.startsWith("file")) {
                    String defaultFSProp = PropertyUtils.getString(Constants.FS_DEFAULT_FS);
                    if (StringUtils.isEmpty(defaultFSProp)) {
                        logger.error("property:{} can not to be empty, please set!", Constants.FS_DEFAULT_FS);
                        throw new StorageOperateNoConfiguredException(
                            String.format("property: %s can not to be empty, please set!",
                                Constants.FS_DEFAULT_FS));
                    }
                    Map<String, String> fsRelatedProps = PropertyUtils.getPrefixedProperties("fs.");
                    configuration.set(Constants.FS_DEFAULT_FS, defaultFSProp);
                    fsRelatedProps.forEach((key, value) -> {
                        configuration.set(key, value);
                        logger.info("Set HDFS prop: {}  -> {}", key, value);
                    });
                } else {
                    logger.info("get property:{} -> {}, from core-site.xml hdfs-site.xml ", Constants.FS_DEFAULT_FS,
                        defaultFS);
                }

                KerberosUtils.loadKerberosConf(configuration);
                fs = FileSystem.get(configuration);

                logger.info("Init HDFS config by kerberos");

            } else if (StringUtils.isNotEmpty(hdfsUser)) {
                logger.info("No kerberos auth way, will create remote user {} for HDFS", hdfsUser);
                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);
                ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> {
                    fs = FileSystem.get(configuration);
                    return true;
                });
                UserGroupInformation.setLoginUser(ugi);
            } else {
                // maybe should throw exception here
                logger.warn("Kerberos startup state is false and hdfs.root.user is not set value!");
            }
        } catch (StorageOperateNoConfiguredException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageOperateNoConfiguredException("Init HDFS config fail", e);
        }
        logger.info("Init HDFS config finish");
    }

    /**
     * @return Configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * cat file on hdfs
     *
     * @param hdfsFilePath hdfs file path
     * @param skipLineNums skip line numbers
     * @param limit        read how many lines
     * @return content of file
     * @throws IOException errors
     */
    public List<String> catFile(String hdfsFilePath, int skipLineNums, int limit) throws IOException {

        if (StringUtils.isBlank(hdfsFilePath)) {
            logger.error("hdfs file path:{} is blank", hdfsFilePath);
            return Collections.emptyList();
        }

        try (FSDataInputStream in = fs.open(new Path(hdfsFilePath))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            Stream<String> stream = br.lines().skip(skipLineNums).limit(limit);
            return stream.collect(Collectors.toList());
        }
    }

    @Override
    public List<String> vimFile(String bucketName, String hdfsFilePath, int skipLineNums,
                                int limit) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> catFile(hdfsFilePath, skipLineNums, limit), configuration);
    }

    @Override
    public void createTenantDirIfNotExists(String tenantCode) throws IOException {
        mkdir(tenantCode, getHdfsResDir(tenantCode));
        mkdir(tenantCode, getHdfsUdfDir(tenantCode));
    }

    @Override
    public String getResDir(String tenantCode) {
        return getHdfsResDir(tenantCode);
    }

    @Override
    public String getUdfDir(String tenantCode) {
        return getHdfsUdfDir(tenantCode);
    }

    /**
     * make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     *
     * @param hdfsPath path to create
     * @return mkdir result
     * @throws IOException errors
     */
    @Override
    public boolean mkdir(String bucketName, String hdfsPath) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> fs.mkdirs(new Path(hdfsPath)), configuration);
    }

    @Override
    public String getResourceFileName(String tenantCode, String fullName) {
        return getHdfsResourceFileName(tenantCode, fullName);
    }

    @Override
    public String getFileName(ResourceType resourceType, String tenantCode, String fileName) {
        return getHdfsFileName(resourceType, tenantCode, fileName);
    }

    @Override
    public void download(String bucketName, String srcHdfsFilePath, String dstFile, boolean deleteSource,
                         boolean overwrite) throws IOException {
        KerberosUtils.doWithReloginIfAuthFail(() -> copyHdfsToLocal(srcHdfsFilePath, dstFile, deleteSource, overwrite),
            configuration);
    }

    /**
     * copy files between FileSystems
     *
     * @param srcPath      source hdfs path
     * @param dstPath      destination hdfs path
     * @param deleteSource whether to delete the src
     * @param overwrite    whether to overwrite an existing file
     * @return if success or not
     * @throws IOException errors
     */
    @Override
    public boolean copy(String srcPath, String dstPath, boolean deleteSource, boolean overwrite) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> FileUtil.copy(fs, new Path(srcPath), fs, new Path(dstPath),
            deleteSource, overwrite, fs.getConf()), configuration);
    }

    /**
     * the src file is on the local disk.  Add it to FS at
     * the given dst name.
     *
     * @param srcFile      local file
     * @param dstHdfsPath  destination hdfs path
     * @param deleteSource whether to delete the src
     * @param overwrite    whether to overwrite an existing file
     * @return if success or not
     * @throws IOException errors
     */
    public boolean copyLocalToHdfs(String srcFile, String dstHdfsPath, boolean deleteSource,
                                   boolean overwrite) throws IOException {
        Path srcPath = new Path(srcFile);
        Path dstPath = new Path(dstHdfsPath);

        KerberosUtils.doWithReloginIfAuthFail(() -> {
            fs.copyFromLocalFile(deleteSource, overwrite, srcPath, dstPath);
            return null;
        }, configuration);

        return true;
    }

    @Override
    public boolean upload(String buckName, String srcFile, String dstPath, boolean deleteSource,
                          boolean overwrite) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> copyLocalToHdfs(srcFile, dstPath, deleteSource, overwrite),
            configuration);
    }

    /**
     * copy hdfs file to local
     *
     * @param srcHdfsFilePath source hdfs file path
     * @param dstFile         destination file
     * @param deleteSource    delete source
     * @param overwrite       overwrite
     * @return result of copy hdfs file to local
     * @throws IOException errors
     */
    public boolean copyHdfsToLocal(String srcHdfsFilePath, String dstFile, boolean deleteSource,
                                   boolean overwrite) throws IOException {
        Path srcPath = new Path(srcHdfsFilePath);
        File dstPath = new File(dstFile);

        if (dstPath.exists()) {
            if (dstPath.isFile()) {
                if (overwrite) {
                    Files.delete(dstPath.toPath());
                }
            } else {
                logger.error("destination file must be a file");
            }
        }

        if (!dstPath.getParentFile().exists() && !dstPath.getParentFile().mkdirs()) {
            return false;
        }

        return FileUtil.copy(fs, srcPath, dstPath, deleteSource, fs.getConf());
    }

    /**
     * delete a file
     *
     * @param hdfsFilePath the path to delete.
     * @param recursive    if path is a directory and set to
     *                     true, the directory is deleted else throws an exception. In
     *                     case of a file the recursive can be set to either true or false.
     * @return true if delete is successful else false.
     * @throws IOException errors
     */
    @Override
    public boolean delete(String tenantCode, String hdfsFilePath, boolean recursive) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> fs.delete(new Path(hdfsFilePath), recursive), configuration);
    }

    /**
     * check if exists
     *
     * @param hdfsFilePath source file path
     * @return result of exists or not
     * @throws IOException errors
     */
    @Override
    public boolean exists(String tenantCode, String hdfsFilePath) throws IOException {
        return KerberosUtils.doWithReloginIfAuthFail(() -> fs.exists(new Path(hdfsFilePath)), configuration);
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     *
     * @param src path to be renamed
     * @param dst new path after rename
     * @return true if rename is successful
     * @throws IOException on failure
     */
    public boolean rename(String src, String dst) throws IOException {
        return fs.rename(new Path(src), new Path(dst));
    }

    /**
     * get data hdfs path
     *
     * @return data hdfs path
     */
    public static String getHdfsDataBasePath() {
        if (Constants.FOLDER_SEPARATOR.equals(RESOURCE_UPLOAD_PATH)) {
            return "";
        } else {
            return RESOURCE_UPLOAD_PATH;
        }
    }

    /**
     * hdfs resource dir
     *
     * @param tenantCode   tenant code
     * @param resourceType resource type
     * @return hdfs resource dir
     */
    public static String getHdfsDir(ResourceType resourceType, String tenantCode) {
        String hdfsDir = "";
        if (resourceType.equals(ResourceType.FILE)) {
            hdfsDir = getHdfsResDir(tenantCode);
        } else if (resourceType.equals(ResourceType.UDF)) {
            hdfsDir = getHdfsUdfDir(tenantCode);
        }
        return hdfsDir;
    }

    @Override
    public String getDir(ResourceType resourceType, String tenantCode) {
        return getHdfsDir(resourceType, tenantCode);
    }

    /**
     * hdfs resource dir
     *
     * @param tenantCode tenant code
     * @return hdfs resource dir
     */
    public static String getHdfsResDir(String tenantCode) {
        return String.format("%s/" + Constants.RESOURCE_TYPE_FILE, getHdfsTenantDir(tenantCode));
    }

    /**
     * hdfs udf dir
     *
     * @param tenantCode tenant code
     * @return get udf dir on hdfs
     */
    public static String getHdfsUdfDir(String tenantCode) {
        return String.format("%s/" + Constants.RESOURCE_TYPE_UDF, getHdfsTenantDir(tenantCode));
    }

    /**
     * get hdfs file name
     *
     * @param resourceType resource type
     * @param tenantCode   tenant code
     * @param fileName     file name
     * @return hdfs file name
     */
    public static String getHdfsFileName(ResourceType resourceType, String tenantCode, String fileName) {
        if (fileName.startsWith(Constants.FOLDER_SEPARATOR)) {
            fileName = fileName.replaceFirst(Constants.FOLDER_SEPARATOR, "");
        }
        return String.format(Constants.FORMAT_S_S, getHdfsDir(resourceType, tenantCode), fileName);
    }

    /**
     * get absolute path and name for resource file on hdfs
     *
     * @param tenantCode tenant code
     * @param fileName   file name
     * @return get absolute path and name for file on hdfs
     */
    public static String getHdfsResourceFileName(String tenantCode, String fileName) {
        if (fileName.startsWith(Constants.FOLDER_SEPARATOR)) {
            fileName = fileName.replaceFirst(Constants.FOLDER_SEPARATOR, "");
        }
        return String.format(Constants.FORMAT_S_S, getHdfsResDir(tenantCode), fileName);
    }

    /**
     * get absolute path and name for udf file on hdfs
     *
     * @param tenantCode tenant code
     * @param fileName   file name
     * @return get absolute path and name for udf file on hdfs
     */
    public static String getHdfsUdfFileName(String tenantCode, String fileName) {
        if (fileName.startsWith(Constants.FOLDER_SEPARATOR)) {
            fileName = fileName.replaceFirst(Constants.FOLDER_SEPARATOR, "");
        }
        return String.format(Constants.FORMAT_S_S, getHdfsUdfDir(tenantCode), fileName);
    }

    /**
     * @param tenantCode tenant code
     * @return file directory of tenants on hdfs
     */
    public static String getHdfsTenantDir(String tenantCode) {
        return String.format(Constants.FORMAT_S_S, getHdfsDataBasePath(), tenantCode);
    }

    @Override
    public void close() throws IOException {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                logger.error("Close HadoopUtils instance failed", e);
                throw new IOException("Close HadoopUtils instance failed", e);
            }
        }
    }

    /**
     * yarn ha admin utils
     */
    public static final class YarnHAAdminUtils extends RMAdminCLI {

        /**
         * get active resourcemanager node
         *
         * @param protocol http protocol
         * @param rmIds    yarn ha ids
         * @return yarn active node
         */
        public static String getActiveRMName(String protocol, String rmIds) {

            String[] rmIdArr = rmIds.split(org.apache.dolphinscheduler.spi.utils.Constants.COMMA);

            String yarnUrl = protocol + "%s:" + HADOOP_RESOURCE_MANAGER_HTTP_ADDRESS_PORT_VALUE + "/ws/v1/cluster/info";

            try {

                /**
                 * send http get request to rm
                 */

                for (String rmId : rmIdArr) {
                    String state = getRMState(String.format(yarnUrl, rmId));
                    if ("ACTIVE".equals(state)) {
                        return rmId;
                    }
                }

            } catch (Exception e) {
                logger.error("yarn ha application url generation failed, message:{}", e.getMessage());
            }
            return null;
        }

        /**
         * get ResourceManager state
         */
        public static String getRMState(String url) {

            String retStr = Boolean.TRUE
                .equals(org.apache.dolphinscheduler.common.utils.PropertyUtils
                    .getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false))
                ? KerberosHttpClient.get(url)
                : HttpUtils.get(url);

            if (org.apache.commons.lang3.StringUtils.isEmpty(retStr)) {
                return null;
            }
            // to json
            ObjectNode jsonObject = org.apache.dolphinscheduler.common.utils.JSONUtils.parseObject(retStr);

            // get ResourceManager state
            if (!jsonObject.has("clusterInfo")) {
                return null;
            }
            return jsonObject.get("clusterInfo").path("haState").asText();
        }

    }

    @Override
    public void deleteTenant(String tenantCode) throws Exception {
        String tenantPath = getHdfsDataBasePath() + Constants.FOLDER_SEPARATOR + tenantCode;

        if (exists(tenantCode, tenantPath)) {
            delete(tenantCode, tenantPath, true);

        }
    }

    @Override
    public ResUploadType returnStorageType() {
        return ResUploadType.HDFS;
    }
}
