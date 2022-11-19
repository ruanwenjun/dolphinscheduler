package org.apache.dolphinscheduler.dao.repository.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.dto.ListingItem;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Repository
public class ProjectDaoImpl implements ProjectDao {

    @Autowired
    private ProjectMapper projectMapper;

    @Override
    public ListingItem<Project> listingProjects(@NonNull Integer pageSize,
                                                @NonNull Integer pageNo,
                                                Set<Integer> projectIds,
                                                String searchVal) {
        if (CollectionUtils.isEmpty(projectIds)) {
            return ListingItem.empty();
        }
        IPage<Project> projects =
                projectMapper.queryProjectListPaging(new Page<>(pageNo, pageSize), projectIds, searchVal);
        return new ListingItem<>(projects.getRecords(), projects.getTotal());
    }

    @Override
    public List<Long> queryProjectCodeByIds(Set<Integer> projectIds) {
        if (CollectionUtils.isEmpty(projectIds)) {
            return Collections.emptyList();
        }
        return projectMapper.queryProjectCodeByIds(projectIds);
    }
}
