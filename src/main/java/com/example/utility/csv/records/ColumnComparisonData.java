package com.example.utility.csv.records;

public record ColumnComparisonData(
        String name,
        String dataType,
        long numOfRowsUpdated,
        float percentOfRowsUpdated
) {
}
