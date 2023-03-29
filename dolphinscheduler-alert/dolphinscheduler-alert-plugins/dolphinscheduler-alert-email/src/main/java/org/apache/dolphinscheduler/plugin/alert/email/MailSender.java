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

package org.apache.dolphinscheduler.plugin.alert.email;

import com.sun.mail.smtp.SMTPProvider;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.dolphinscheduler.alert.api.AlertConstants;
import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.alert.api.ShowType;
import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.TaskResultAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.plugin.alert.email.exception.AlertEmailException;
import org.apache.dolphinscheduler.plugin.alert.email.template.AlertTemplate;
import org.apache.dolphinscheduler.plugin.alert.email.template.DefaultHTMLTemplate;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.CommandMap;
import javax.activation.MailcapCommandMap;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public final class MailSender {

    private static final Logger logger = LoggerFactory.getLogger(MailSender.class);

    private final List<String> receivers;
    private final List<String> receiverCcs;
    private final String mailProtocol = "SMTP";
    private final String mailSmtpHost;
    private final String mailSmtpPort;
    private final String mailSenderEmail;
    private final String enableSmtpAuth;
    private final String mailUser;
    private final String mailPasswd;
    private final String mailUseStartTLS;
    private final String mailUseSSL;
    private final String sslTrust;
    private final String showType;
    private final AlertTemplate alertTemplate;
    private final String mustNotNull = " must not be null";
    private String xlsFilePath;

    public MailSender(Map<String, String> config) {
        String receiversConfig = config.get(MailParamsConstants.NAME_PLUGIN_DEFAULT_EMAIL_RECEIVERS);
        if (receiversConfig == null || "".equals(receiversConfig)) {
            throw new AlertEmailException(MailParamsConstants.NAME_PLUGIN_DEFAULT_EMAIL_RECEIVERS + mustNotNull);
        }

        receivers = Arrays.asList(receiversConfig.split(","));

        String receiverCcsConfig = config.get(MailParamsConstants.NAME_PLUGIN_DEFAULT_EMAIL_RECEIVERCCS);

        receiverCcs = new ArrayList<>();
        if (receiverCcsConfig != null && !"".equals(receiverCcsConfig)) {
            receiverCcs.addAll(Arrays.asList(receiverCcsConfig.split(",")));
        }

        mailSmtpHost = config.get(MailParamsConstants.NAME_MAIL_SMTP_HOST);
        requireNonNull(mailSmtpHost, MailParamsConstants.NAME_MAIL_SMTP_HOST + mustNotNull);

        mailSmtpPort = config.get(MailParamsConstants.NAME_MAIL_SMTP_PORT);
        requireNonNull(mailSmtpPort, MailParamsConstants.NAME_MAIL_SMTP_PORT + mustNotNull);

        mailSenderEmail = config.get(MailParamsConstants.NAME_MAIL_SENDER);
        requireNonNull(mailSenderEmail, MailParamsConstants.NAME_MAIL_SENDER + mustNotNull);

        enableSmtpAuth = config.get(MailParamsConstants.NAME_MAIL_SMTP_AUTH);

        mailUser = config.get(MailParamsConstants.NAME_MAIL_USER);
        requireNonNull(mailUser, MailParamsConstants.NAME_MAIL_USER + mustNotNull);

        mailPasswd = config.get(MailParamsConstants.NAME_MAIL_PASSWD);
        requireNonNull(mailPasswd, MailParamsConstants.NAME_MAIL_PASSWD + mustNotNull);

        mailUseStartTLS = config.get(MailParamsConstants.NAME_MAIL_SMTP_STARTTLS_ENABLE);
        requireNonNull(mailUseStartTLS, MailParamsConstants.NAME_MAIL_SMTP_STARTTLS_ENABLE + mustNotNull);

        mailUseSSL = config.get(MailParamsConstants.NAME_MAIL_SMTP_SSL_ENABLE);
        requireNonNull(mailUseSSL, MailParamsConstants.NAME_MAIL_SMTP_SSL_ENABLE + mustNotNull);

        sslTrust = config.get(MailParamsConstants.NAME_MAIL_SMTP_SSL_TRUST);
        requireNonNull(sslTrust, MailParamsConstants.NAME_MAIL_SMTP_SSL_TRUST + mustNotNull);

        showType = config.get(AlertConstants.NAME_SHOW_TYPE);
        requireNonNull(showType, AlertConstants.NAME_SHOW_TYPE + mustNotNull);

        xlsFilePath = config.get(EmailConstants.XLS_FILE_PATH);
        if (StringUtils.isBlank(xlsFilePath)) {
            xlsFilePath = "/tmp/xls";
        }

        alertTemplate = new DefaultHTMLTemplate();
    }

    public AlertResult sendMails(AlertInfo info) {
        return sendMails(this.receivers, this.receiverCcs, info);
    }

    public AlertResult sendMails(List<String> receivers, List<String> receiverCcs, AlertInfo info) {
        AlertResult alertResult = new AlertResult();
        alertResult.setSuccess(false);

        AlertContent alertContent = info.getAlertContent();

        // if there is no receivers && no receiversCc, no need to process
        if (CollectionUtils.isEmpty(receivers) && CollectionUtils.isEmpty(receiverCcs)) {
            return alertResult;
        }

        receivers.removeIf(StringUtils::isEmpty);
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        // only result support other format
        if (alertContent.getAlertType() != AlertType.TASK_RESULT) {
            alertResult.setSuccess(true);
            return directlySendAlert(info, alertResult, receivers, receiverCcs);
        }

        TaskResultAlertContent taskResultAlertContent = (TaskResultAlertContent) alertContent;

        String title = info.getAlertData().getTitle();
        String content = JSONUtils.toJsonString(taskResultAlertContent.getResult());
        if (showType.equals(ShowType.TEXT.getDescp())) {
            // send email
            HtmlEmail email = new HtmlEmail();

            try {
                Session session = getSession();
                email.setMailSession(session);
                email.setFrom(mailSenderEmail);
                email.setCharset(EmailConstants.UTF_8);
                if (CollectionUtils.isNotEmpty(receivers)) {
                    // receivers mail
                    for (String receiver : receivers) {
                        email.addTo(receiver);
                    }
                }

                if (CollectionUtils.isNotEmpty(receiverCcs)) {
                    // cc
                    for (String receiverCc : receiverCcs) {
                        email.addCc(receiverCc);
                    }
                }
                // sender mail
                return getStringObjectMap(info.getAlertContent().getAlertTitle(), content, alertResult, email);
            } catch (Exception e) {
                handleException(alertResult, e);
            }
        } else if (showType.equals(ShowType.ATTACHMENT.getDescp())) {
            try {
                attachment(title, content);

                alertResult.setSuccess(true);
                return alertResult;
            } catch (Exception e) {
                handleException(alertResult, e);
                return alertResult;
            }
        }
        return alertResult;

    }

    /**
     * send mail as Excel attachment
     */
    private void attachment(String title, String content) throws Exception {
        MimeMessage msg = getMimeMessage();

        attachContent(title, content, msg);
    }

    /**
     * get MimeMessage
     */
    private MimeMessage getMimeMessage() throws MessagingException {

        // 1. The first step in creating mail: creating session
        Session session = getSession();
        // Setting debug mode, can be turned off
        session.setDebug(false);

        // 2. creating mail: Creating a MimeMessage
        MimeMessage msg = new MimeMessage(session);
        // 3. set sender
        msg.setFrom(new InternetAddress(mailSenderEmail));
        // 4. set receivers
        for (String receiver : receivers) {
            msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(receiver));
        }
        return msg;
    }

    /**
     * get session
     *
     * @return the new Session
     */
    private Session getSession() {
        // support multilple email format
        MailcapCommandMap mc = (MailcapCommandMap) CommandMap.getDefaultCommandMap();
        mc.addMailcap("text/html;; x-java-content-handler=com.sun.mail.handlers.text_html");
        mc.addMailcap("text/xml;; x-java-content-handler=com.sun.mail.handlers.text_xml");
        mc.addMailcap("text/plain;; x-java-content-handler=com.sun.mail.handlers.text_plain");
        mc.addMailcap("multipart/*;; x-java-content-handler=com.sun.mail.handlers.multipart_mixed");
        mc.addMailcap("message/rfc822;; x-java-content-handler=com.sun.mail.handlers.message_rfc822");
        CommandMap.setDefaultCommandMap(mc);

        Properties props = new Properties();
        props.setProperty(MailParamsConstants.MAIL_SMTP_HOST, mailSmtpHost);
        props.setProperty(MailParamsConstants.MAIL_SMTP_PORT, mailSmtpPort);
        props.setProperty(MailParamsConstants.MAIL_SMTP_AUTH, enableSmtpAuth);
        props.setProperty(EmailConstants.MAIL_TRANSPORT_PROTOCOL, mailProtocol);
        props.setProperty(MailParamsConstants.MAIL_SMTP_STARTTLS_ENABLE, mailUseStartTLS);
        props.setProperty(MailParamsConstants.MAIL_SMTP_SSL_ENABLE, mailUseSSL);
        props.setProperty(MailParamsConstants.MAIL_SMTP_SSL_TRUST, sslTrust);

        Authenticator auth = new Authenticator() {

            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                // mail username and password
                return new PasswordAuthentication(mailUser, mailPasswd);
            }
        };

        Session session = Session.getInstance(props, auth);
        session.addProvider(new SMTPProvider());
        return session;
    }

    /**
     * attach content
     */
    private void attachContent(String title, String content, MimeMessage msg) throws MessagingException, IOException {
        title = title.replace(" ", "_");
        /*
         * set receiverCc
         */
        if (CollectionUtils.isNotEmpty(receiverCcs)) {
            for (String receiverCc : receiverCcs) {
                msg.addRecipients(Message.RecipientType.CC, InternetAddress.parse(receiverCc));
            }
        }

        // set subject
        msg.setSubject("WhaleScheduler Alert");
        MimeMultipart partList = new MimeMultipart();
        // set signature
        MimeBodyPart part1 = new MimeBodyPart();
        part1.setContent("see more detail in attachment", EmailConstants.TEXT_HTML_CHARSET_UTF_8);
        // set attach file
        MimeBodyPart part2 = new MimeBodyPart();
        // add random uuid to filename to avoid potential issue
        String randomFilename = title + UUID.randomUUID();
        File file =
            new File(xlsFilePath + EmailConstants.SINGLE_SLASH + randomFilename + EmailConstants.TEXT_SUFFIX);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
            writer.flush();
        } catch (IOException e) {
            logger.error("write file error", e);
            throw e;
        }

        part2.attachFile(file);
        part2.setFileName(MimeUtility.encodeText(title + EmailConstants.TEXT_SUFFIX, EmailConstants.UTF_8, "B"));
        // add components to collection
        partList.addBodyPart(part1);
        partList.addBodyPart(part2);
        msg.setContent(partList);
        // 5. send Transport
        Transport.send(msg);
        // 6. delete saved file
        deleteFile(file);
    }

    /**
     * the string object map
     */
    private AlertResult getStringObjectMap(String title, String content, AlertResult alertResult,
                                           HtmlEmail email) throws EmailException {

        /*
         * the subject of the message to be sent
         */
        email.setSubject(title);
        /*
         * to send information, you can use HTML tags in mail content because of the use of HtmlEmail
         */
        String contentTemp = alertTemplate.getMessageFromTemplate(content, ShowType.TEXT);
        email.setMsg(contentTemp);

        // send
        email.setDebug(true);
        email.send();

        alertResult.setSuccess(true);

        return alertResult;
    }

    /**
     * file delete
     *
     * @param file the file to delete
     */
    public void deleteFile(File file) {
        if (file.exists()) {
            if (file.delete()) {
                logger.info("delete success: {}", file.getAbsolutePath());
            } else {
                logger.info("delete fail: {}", file.getAbsolutePath());
            }
        } else {
            logger.info("file not exists: {}", file.getAbsolutePath());
        }
    }

    /**
     * handle exception
     */
    private void handleException(AlertResult alertResult, Exception e) {
        logger.error("Send email to {} failed", receivers, e);
        alertResult.setMessage("Send email to {" + String.join(",", receivers) + "} failed，" + e.toString());
    }

    private AlertResult directlySendAlert(AlertInfo alertInfo,
                                          AlertResult alertResult,
                                          List<String> receivers,
                                          List<String> receiverCcs) {
        AlertData alertData = alertInfo.getAlertData();
        String title = alertData.getTitle();
        String content = alertData.getContent();
        // send email
        HtmlEmail email = new HtmlEmail();

        try {
            Session session = getSession();
            email.setMailSession(session);
            email.setFrom(mailSenderEmail);
            email.setCharset(EmailConstants.UTF_8);
            if (CollectionUtils.isNotEmpty(receivers)) {
                // receivers mail
                for (String receiver : receivers) {
                    email.addTo(receiver);
                }
            }

            if (CollectionUtils.isNotEmpty(receiverCcs)) {
                // cc
                for (String receiverCc : receiverCcs) {
                    email.addCc(receiverCc);
                }
            }
            /*
             * the subject of the message to be sent
             */
            email.setSubject(title);
            /*
             * to send information, you can use HTML tags in mail content because of the use of HtmlEmail
             */
            String contentTemp = alertTemplate.getMessageFromTemplate(content, ShowType.TEXT);
            email.setMsg(contentTemp);

            // send
            email.setDebug(true);
            email.send();

            alertResult.setSuccess(true);

        } catch (Exception e) {
            handleException(alertResult, e);
        }
        return alertResult;
    }

}
