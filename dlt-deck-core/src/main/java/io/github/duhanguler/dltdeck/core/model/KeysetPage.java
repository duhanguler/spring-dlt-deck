package io.github.duhanguler.dltdeck.core.model;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record KeysetPage<T>(List<T> items, @Nullable String nextCursor, boolean hasMore) {

    public KeysetPage {
        items = List.copyOf(items);
    }
}
