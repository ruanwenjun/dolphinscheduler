package org.apache.dolphinscheduler.alert.content;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TemplateInjectedAlertContentWrapper implements AlertContentWrapper {

    private AlertContent alertContentPojo;
    private String alertTitle;
    private String alertContent;
}
