package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.ServerCrashAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class ServerCrashTemplateAlertContentInjector extends BaseAlertTemplateInjector {

    public ServerCrashTemplateAlertContentInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        ServerCrashAlertContent serverCrashAlertContent =
                (ServerCrashAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        serverCrashAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.SERVER_PATH_TEMPLATE, serverCrashAlertContent.getServerPath());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        serverCrashAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.SERVER_PATH_TEMPLATE, serverCrashAlertContent.getServerPath());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertTitle(title)
                .alertContent(content)
                .alertContentPojo(alertContent)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.SERVER_CRASH_ALERT;
    }
}
