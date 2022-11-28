package org.apache.dolphinscheduler.alert.content.template;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertTemplate {

    private String titleTemplate;
    private String contentTemplate;
}
