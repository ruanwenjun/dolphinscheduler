package org.apache.dolphinscheduler.dao.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ListingItem<T> {

    private List<T> items;

    private long totalCount;

    public static <T> ListingItem<T> empty() {
        ListingItem<T> result = new ListingItem<>();
        result.setItems(Collections.emptyList());
        return result;
    }

}
