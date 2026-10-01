package com.school.selfcheckout.analytics.pipeline;

import com.school.selfcheckout.domain.PopularEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stage 2: counts scans per SKU in a window snapshot and keeps the top
 * {@code maxRanks}. Stateless -- each snapshot is ranked on its own.
 */
final class RankFilter extends Filter<WindowSnapshot, RankedWindow> {

    private static final Comparator<Map.Entry<String, Integer>> BY_COUNT_THEN_SKU =
            Comparator.<Map.Entry<String, Integer>>comparingInt(Map.Entry::getValue).reversed()
                    // Tie-break by SKU so equal counts rank deterministically.
                    .thenComparing(Map.Entry::getKey);

    private final int maxRanks;

    RankFilter(Pipe<WindowSnapshot> input, Pipe<RankedWindow> output, int maxRanks) {
        super("rank", input, output);
        this.maxRanks = maxRanks;
    }

    @Override
    protected void process(WindowSnapshot snapshot) {
        Map<String, Integer> counts = new HashMap<>();
        for (String sku : snapshot.skus()) {
            if (sku != null) {
                counts.merge(sku, 1, Integer::sum);
            }
        }

        List<Map.Entry<String, Integer>> ranked = counts.entrySet().stream()
                .sorted(BY_COUNT_THEN_SKU)
                .limit(maxRanks)
                .toList();

        List<PopularEntry> entries = new ArrayList<>(ranked.size());
        for (int i = 0; i < ranked.size(); i++) {
            entries.add(new PopularEntry(i + 1, ranked.get(i).getKey(), ranked.get(i).getValue()));
        }

        emit(new RankedWindow(snapshot.windowStart(), snapshot.windowEnd(), entries));
    }
}
