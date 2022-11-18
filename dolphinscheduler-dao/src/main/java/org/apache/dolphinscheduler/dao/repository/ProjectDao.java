package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.dto.ListingItem;
import org.apache.dolphinscheduler.dao.entity.Project;

import java.util.Set;

public interface ProjectDao {

    ListingItem<Project> listingProjects(@NonNull Integer pageSize, @NonNull Integer pageNo, Set<Integer> projectIds,
                                         String searchVal);

}
