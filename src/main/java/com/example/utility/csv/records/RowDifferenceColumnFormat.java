package com.example.utility.csv.records;

public record RowDifferenceColumnFormat(
        String columnName,
        String primaryKey,
        String oldValue,
        String newValue
) {}
