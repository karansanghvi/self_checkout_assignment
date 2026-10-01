package com.school.selfcheckout.analytics.pipeline;

import com.school.selfcheckout.domain.PopularEntry;

import java.util.List;

/** One hop of the window, reduced to its top-N SKUs by scan count. */
record RankedWindow(long windowStart, long windowEnd, List<PopularEntry> entries) {
}
