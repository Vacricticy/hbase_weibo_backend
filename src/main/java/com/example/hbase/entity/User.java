package com.example.hbase.entity;

public class User {
    String row_key;
    String info_gender;
    String info_nickname;
    String state;

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }

    public String getInfo_gender() {
        return info_gender;
    }

    public void setInfo_gender(String info_gender) {
        this.info_gender = info_gender;
    }

    public String getInfo_nickname() {
        return info_nickname;
    }

    public void setInfo_nickname(String info_nickname) {
        this.info_nickname = info_nickname;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "User{" +
                "row_key='" + row_key + '\'' +
                ", info_gender='" + info_gender + '\'' +
                ", info_nickname='" + info_nickname + '\'' +
                ", state='" + state + '\'' +
                '}';
    }
}
