package com.example.hbase.result;

public class ResultDept {
    String row_key;
    String column_family;
    String column;
    String value;

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }

    public String getColumn_family() {
        return column_family;
    }

    public void setColumn_family(String column_family) {
        this.column_family = column_family;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
