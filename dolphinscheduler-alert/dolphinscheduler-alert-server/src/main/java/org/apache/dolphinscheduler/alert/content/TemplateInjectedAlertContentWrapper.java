package org.apache.dolphinscheduler.alert.content;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TemplateInjectedAlertContentWrapper implements AlertContentWrapper {

    private String alertTitle;
    private String alertContent;
}
