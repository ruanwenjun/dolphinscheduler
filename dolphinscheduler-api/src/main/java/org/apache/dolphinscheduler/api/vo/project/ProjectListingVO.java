package org.apache.dolphinscheduler.api.vo.project;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.Project;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProjectListingVO {

    private int id;

    private int userId;

    private String userName;

    private long code;

    private String name;

    private String description;

    private Date createTime;

    private Date updateTime;

    private int perm;

    public ProjectListingVO(@NonNull Project project) {
        this.id = project.getId();
        this.userId = project.getUserId();
        this.code = project.getCode();
        this.name = project.getName();
        this.description = project.getDescription();
        this.createTime = project.getCreateTime();
        this.updateTime = project.getUpdateTime();
    }
}
