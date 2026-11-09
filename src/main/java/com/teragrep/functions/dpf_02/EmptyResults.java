package com.teragrep.functions.dpf_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Objects;

final class EmptyResults implements SearchResults {

    private final int capacity;
    private final Sort sort;

    EmptyResults(final int capacity, final Sort sort) {
        this.capacity = capacity;
        this.sort = Objects.requireNonNull(sort);
    }

    @Override
    public SearchResults including(final Dataset<Row> batch, final Limits limits) {
        final Dataset<Row> sorted = sort.apply(batch);
        final Dataset<Row> limited = limits.incoming(sorted);
        final List<Row> selected = limited.collectAsList();
        return new TopSearchResults(
                capacity,
                sort,
                selected,
                batch.schema(),
                batch.sparkSession()
        );
    }

    @Override
    public Dataset<Row> dataset() {
        throw new UnsupportedOperationException("cannot create a dataset from empty results");
    }

}
