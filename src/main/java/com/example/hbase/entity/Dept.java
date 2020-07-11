package com.example.hbase.entity;

public class Dept {
    String row_key;
    String info_name;
    String info_fpid;
    String subdept;

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }

    public String getInfo_name() {
        return info_name;
    }

    public void setInfo_name(String info_name) {
        this.info_name = info_name;
    }

    public String getInfo_fpid() {
        return info_fpid;
    }

    public void setInfo_fpid(String info_fpid) {
        this.info_fpid = info_fpid;
    }

    public String getSubdept() {
        return subdept;
    }

    public void setSubdept(String subdept) {
        this.subdept = subdept;
    }
}
