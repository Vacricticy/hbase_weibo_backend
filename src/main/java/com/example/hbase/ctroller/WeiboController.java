

package com.example.hbase.ctroller;

import com.example.hbase.entity.Dept;
import com.example.hbase.entity.User;
import com.example.hbase.result.ResultDept;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Controller
public class WeiboController {

    @CrossOrigin
    @PostMapping(value = "api/getFollowingUsers")
    @ResponseBody
    public List<User> getFollowingUsers(@RequestBody User requestuser) {

        System.out.println("come in");
        String row_key = requestuser.getRow_key();
        List<User> deptList = new ArrayList<>();
        List<User> followingUsers = new ArrayList<>();

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("weibo:users"));
            FilterList filterList = new FilterList();
//            1.过滤行键为登录用户id的数据
            RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(row_key));
            filterList.addFilter(rf);
//            2.过滤出列族为following（关注记录）的数据
            FamilyFilter ff = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("following")));
            filterList.addFilter(ff);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            List<String> followingUsersRowKey = new ArrayList<>();
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    followingUsersRowKey.add(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            System.out.println(followingUsersRowKey);

//            根据前面获取的关注列表一一获取其个人信息
            for (int i = 0; i < followingUsersRowKey.size(); i++) {
                Scan scan2 = new Scan();
                FilterList filterList2 = new FilterList();
                //1.过滤出行键为关注用户id的数据
                RowFilter rf2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(followingUsersRowKey.get(i)));
                filterList2.addFilter(rf2);
//            2.过滤出列族为info的数据
//                FamilyFilter ff2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
//                filterList2.addFilter(ff2);

                scan2.setFilter(filterList2);
                ResultScanner resultScanner2 = table.getScanner(scan2);
                for (Result result : resultScanner2) {
                    List<Cell> cells = result.listCells();
                    User user = new User();
                    int flag = 0;
                    for (Cell cell : cells) {
                        user.setRow_key(Bytes.toString(result.getRow()));
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("gender")) {
                            if (Bytes.toString(CellUtil.cloneValue(cell)).equals("1")) {
                                user.setInfo_gender("男");
                            } else {
                                user.setInfo_gender("女");
                            }
                        }
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("nickname")) {
                            user.setInfo_nickname(Bytes.toString(CellUtil.cloneValue(cell)));
                        }
//                        判断所关注用户是否也关注了本人
                        if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("following")) {
                            if (Bytes.toString(CellUtil.cloneValue(cell)).equals(row_key)) {
                                user.setState("互相关注");
                                flag = 1;
                            }
                        }
                        if (flag != 1) {
                            user.setState("已关注");
                        }
                        String row = Bytes.toString(result.getRow());
                        String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");
                    }
                    followingUsers.add(user);
                }
            }
            System.out.println(followingUsers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return followingUsers;
    }


    @CrossOrigin
    @PostMapping(value = "api/getFollowers")
    @ResponseBody
    public List<User> getFollowers(@RequestBody User requestuser) {

        System.out.println("come in");
        String row_key = requestuser.getRow_key();
        List<User> deptList = new ArrayList<>();
        List<User> followers = new ArrayList<>();

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("weibo:users"));
            FilterList filterList = new FilterList();

//            1.过滤获取粉丝的row_key
            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("following"), Bytes.toBytes(row_key), CompareFilter.CompareOp.EQUAL, row_key.getBytes());
//            设置为true表示当这一列不存在时，不会返回
            scvf.setFilterIfMissing(true);
            filterList.addFilter(scvf);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            List<String> followersRowKey = new ArrayList<>();
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    if (!followersRowKey.contains(Bytes.toString(result.getRow()))) {
                        followersRowKey.add(Bytes.toString(result.getRow()));
                    }
//                    String row = Bytes.toString(result.getRow());
//                    String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
//                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//                    System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");
                }
            }
            System.out.println(followersRowKey);

//            2.根据粉丝的row_key获取粉丝的信息
            for (int i = 0; i < followersRowKey.size(); i++) {
                Scan scan2 = new Scan();
                FilterList filterList2 = new FilterList();
                //1.根据粉丝的row_key获取其信息
                RowFilter rf2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(followersRowKey.get(i)));
                filterList2.addFilter(rf2);

                scan2.setFilter(filterList2);
                ResultScanner resultScanner2 = table.getScanner(scan2);
                for (Result result : resultScanner2) {
                    List<Cell> cells = result.listCells();
                    User user = new User();
                    int flag = 0;
                    for (Cell cell : cells) {
                        user.setRow_key(Bytes.toString(result.getRow()));
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("gender")) {
                            if (Bytes.toString(CellUtil.cloneValue(cell)).equals("1")) {
                                user.setInfo_gender("男");
                            } else {
                                user.setInfo_gender("女");
                            }
                        }
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("nickname")) {
                            user.setInfo_nickname(Bytes.toString(CellUtil.cloneValue(cell)));
                        }
//                        判断登录用户是否关注了粉丝
                        if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("followers")) {
                            if (Bytes.toString(CellUtil.cloneValue(cell)).equals(row_key)) {
                                user.setState("互相关注");
                                flag = 1;
                            }
                        }
                        if (flag != 1) {
                            user.setState("已关注");
                        }
                        String row = Bytes.toString(result.getRow());
                        String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");
                    }
                    followers.add(user);
                }
                System.out.println(followers);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return followers;
    }


    @CrossOrigin
    @PostMapping(value = "api/searchUser")
    @ResponseBody
    public List<User> searchUser(@RequestBody User requestuser) {

        System.out.println("come in");
        String nickname = requestuser.getInfo_nickname();
        String login_row_key = requestuser.getRow_key();
        List<User> deptList = new ArrayList<>();
        List<User> searchResult = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("weibo:users"));


//            1.获取搜索到的用户
            FilterList filterList = new FilterList();
//            列值过滤器+设置字符串匹配比较器
            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("nickname"), CompareFilter.CompareOp.EQUAL, new SubstringComparator(nickname));
            filterList.addFilter(scvf);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            ResultScanner resultScanner = table.getScanner(scan);
            List<String> followingUsersRowKey = new ArrayList<>();
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                User user = new User();
                int flag = 0;//flag为0表示双方谁都没有关注谁，flag为1表示登录用户关注了搜索到的用户，flag为2表示搜索到的用户关注了登录用户，flag=3表示双方为互相关注
                for (Cell cell : cells) {
//                    判断原登录用户是否关注了搜索到的用户
                    FilterList filterList2 = new FilterList();
                    RowFilter rf2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(login_row_key));
                    filterList2.addFilter(rf2);
                    Scan scan2 = new Scan();
                    scan2.setFilter(filterList2);
                    ResultScanner resultScanner2 = table.getScanner(scan2);
                    for (Result result2 : resultScanner2) {
                        List<Cell> cells2 = result2.listCells();
                        for (Cell cell2 : cells2) {

                            if (Bytes.toString(CellUtil.cloneFamily(cell2)).equals("following")) {
//                                判断登录用户的following列的值是否等于搜索到的用户的row_key,如果相等则表示登录用户关注了搜索到的用户
                                if (Bytes.toString(CellUtil.cloneValue(cell2)).equals(Bytes.toString(result.getRow()))) {
//                                    user.setState("互相关注");
                                    if (flag != 3) {
                                        flag = 1;
                                    }

                                }
                            }
//                            System.out.println(Bytes.toString(CellUtil.cloneValue(cell2)));
//                            System.out.println(Bytes.toString(result.getRow()));
//                            String row = Bytes.toString(result2.getRow());
//                            String family1 = Bytes.toString(CellUtil.cloneFamily(cell2));
//                            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell2));
//                            String value = Bytes.toString(CellUtil.cloneValue(cell2));
//                            System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");
                        }
                    }

                    user.setRow_key(Bytes.toString(result.getRow()));
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("gender")) {
                        if (Bytes.toString(CellUtil.cloneValue(cell)).equals("1")) {
                            user.setInfo_gender("男");
                        } else {
                            user.setInfo_gender("女");
                        }
                    }
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("nickname")) {
                        user.setInfo_nickname(Bytes.toString(CellUtil.cloneValue(cell)));
                    }
//                        判断搜索到的用户是否已经关注原登录用户
                    System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                    if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("following")) {
                        if (Bytes.toString(CellUtil.cloneValue(cell)).equals(login_row_key)) {
                            if (flag == 1) {
                                user.setState("互相关注");
                                flag = 3;
                            }
                        }
                    }

                    if (flag == 0) {
                        user.setState("关注");
                    } else if (flag == 1) {
                        user.setState("已关注");
                    }
                    System.out.println(user.getState());
                    String row = Bytes.toString(result.getRow());
                    String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");
                }
                searchResult.add(user);
            }
            System.out.println(followingUsersRowKey);

            System.out.println(searchResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return searchResult;
    }





    @CrossOrigin
    @PostMapping(value = "api/addFollowing")
    @ResponseBody
    public int addFollowing(@RequestBody Map<String,String> map) {

        System.out.println("come in");
        String login_user_row_key=map.get("login_user_row_key");
        String search_row_key=map.get("search_row_key");
        System.out.println(login_user_row_key+"  "+search_row_key);


        List<User> deptList = new ArrayList<>();
        List<User> searchResult = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("weibo:users"));
//            添加登录用户的关注记录
            Put put=new Put(Bytes.toBytes(login_user_row_key));
            put.addColumn(Bytes.toBytes("following"),Bytes.toBytes(search_row_key),Bytes.toBytes(search_row_key));
            put.setDurability(Durability.SYNC_WAL);
            table.put(put);
//            添加被关注用户的粉丝记录
            Put put2=new Put(Bytes.toBytes(search_row_key));
            put2.addColumn(Bytes.toBytes("followers"),Bytes.toBytes(login_user_row_key),Bytes.toBytes(login_user_row_key));
            put2.setDurability(Durability.SYNC_WAL);
            table.put(put2);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 999;
    }



    @CrossOrigin
    @PostMapping(value = "api/unFollowing")
    @ResponseBody
    public int unFollowing(@RequestBody Map<String,String> map) {

        System.out.println("come in");
        String login_user_row_key=map.get("login_user_row_key");
        String unfollow_row_key=map.get("unfollow_row_key");
        System.out.println(login_user_row_key+"  "+unfollow_row_key);


        List<User> deptList = new ArrayList<>();
        List<User> searchResult = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("weibo:users"));
//            删除登录用户的关注记录
            Delete delete =new Delete(Bytes.toBytes(login_user_row_key));
            delete.deleteColumn(Bytes.toBytes("following"),Bytes.toBytes(unfollow_row_key));
            table.delete(delete);
//                删除所关注用户的粉丝记录
            Delete delete2 =new Delete(Bytes.toBytes(unfollow_row_key));
            delete2.deleteColumn(Bytes.toBytes("followers"),Bytes.toBytes(login_user_row_key));
            table.delete(delete2);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 999;
    }
}

