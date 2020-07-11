
package com.example.hbase.ctroller;

import com.example.hbase.entity.Dept;
import com.example.hbase.result.ResultDept;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Controller
public class HBaseController {

    @CrossOrigin
    @PostMapping(value = "api/getAllData")
    @ResponseBody
    public List<ResultDept> getAllData(@RequestBody Dept requestDept) {

        System.out.println("come in");
        List<ResultDept> deptList = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();


            HTable table = new HTable(conf, "company:dept");
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            // 输出结果
            for (Result result : results) {
                for (KeyValue rowKV : result.raw()) {
                    ResultDept oneDept1 = new ResultDept();
                    oneDept1.setRow_key(new String(rowKV.getRow()) + " ");
                    oneDept1.setColumn_family(new String(rowKV.getFamily()) + " ");
                    oneDept1.setColumn(new String(rowKV.getQualifier()) + " ");
                    oneDept1.setValue(new String(rowKV.getValue()));
                    deptList.add(oneDept1);
//                    System.out.print("行名:" + new String(rowKV.getRow()) + " ");
//                    System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
//                    System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
//                    System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
//                    System.out.println("值:" + new String(rowKV.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return deptList;
    }


    @CrossOrigin
    @PostMapping(value = "api/getFirstLevelDept")
    @ResponseBody
    public List<ResultDept> getFirstLevelDept(@RequestBody Dept requestDept) {
        System.out.println("come in");
        List<ResultDept> deptList = new ArrayList<>();

        try{
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("company:dept"));
            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), CompareFilter.CompareOp.EQUAL, "".getBytes());
            Scan scan = new Scan();
            scan.setFilter(scvf);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    ResultDept oneDept1 = new ResultDept();
                    oneDept1.setRow_key(Bytes.toString(result.getRow()));
                    oneDept1.setColumn_family(Bytes.toString(CellUtil.cloneFamily(cell)));
                    oneDept1.setColumn(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    oneDept1.setValue(Bytes.toString(CellUtil.cloneValue(cell)));
                    deptList.add(oneDept1);

                    String row = Bytes.toString(result.getRow());
                    String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//                    System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");

                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return deptList;
    }


    @CrossOrigin
    @PostMapping(value = "api/getChildDept")
    @ResponseBody
    public List<ResultDept> getChildDept(@RequestBody ResultDept parentDept) {
        System.out.println("come in");
        List<ResultDept> deptList = new ArrayList<>();
        String parentDeptId=parentDept.getRow_key();
        System.out.println(parentDeptId);
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();


            Table table = connection.getTable(TableName.valueOf("company:dept"));
            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), CompareFilter.CompareOp.EQUAL, parentDeptId.getBytes());
            Scan scan = new Scan();
            scan.setFilter(scvf);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {

                    if(!Bytes.toString(result.getRow()).substring(0,1).equals("0")){
                        ResultDept oneDept1 = new ResultDept();
                        oneDept1.setRow_key(Bytes.toString(result.getRow()));
                        oneDept1.setColumn_family(Bytes.toString(CellUtil.cloneFamily(cell)));
                        oneDept1.setColumn(Bytes.toString(CellUtil.cloneQualifier(cell)));
                        oneDept1.setValue(Bytes.toString(CellUtil.cloneValue(cell)));
                        deptList.add(oneDept1);
                    }


                    String row = Bytes.toString(result.getRow());
                    String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("[row:" + row + "],[family:" + family1 + "],[qualifier:" + qualifier + "]" + ",[value:" + value + "],[time:" + cell.getTimestamp() + "]");

                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return deptList;
    }

    @CrossOrigin
    @PostMapping(value = "api/addDept")
    @ResponseBody
    public int addDept(@RequestBody ResultDept addDeptForm) {
        System.out.println("come in");
        List<ResultDept> deptList = new ArrayList<>();
        String row=addDeptForm.getRow_key();
        String columnFamily=addDeptForm.getColumn_family();
        String column=addDeptForm.getColumn();
        String value=addDeptForm.getValue();
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("company:dept"));
//        在父部门上添加新创建的子部门的信息
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                    Bytes.toBytes(value));
            table.put(put);

//        单独创建新的部门数据
            Put put2 = new Put(Bytes.toBytes(column));
            put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    Bytes.toBytes(value));
            table.put(put2);

            Put put3 = new Put(Bytes.toBytes(column));
            put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f_pid"),
                    Bytes.toBytes(row));
            table.put(put3);

            table.close();

        }catch (Exception e){
            e.printStackTrace();
        }

        return 200;
    }


    @CrossOrigin
    @PostMapping(value = "api/deleteDept")
    @ResponseBody
    public int deleteDept(@RequestBody ResultDept parentDept) {
        System.out.println("come in");
        List<ResultDept> deptList = new ArrayList<>();
        try{
            String deptId=parentDept.getRow_key();
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "121.36.7.141");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf("company:dept"));
//        1.得到原部门的子部门的信息
            ArrayList<String> childDeptsId=new ArrayList<String>();
            ArrayList<String> childDeptsValue=new ArrayList<String>();
            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("f_pid"), CompareFilter.CompareOp.EQUAL, "1_003".getBytes());
            Scan scan = new Scan();
            scan.setFilter(scvf);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String row = Bytes.toString(result.getRow());
                    String value= Bytes.toString(CellUtil.cloneValue(cell));
                    if(!row.substring(0,1).equals("0")&&!childDeptsId.contains(row)){
                        childDeptsId.add(row);
                    }
                    if(childDeptsId.contains(row)&&!value.equals("1_003")){
                        childDeptsValue.add(value);
                    }
                }
            }
            System.out.println(childDeptsId);
            System.out.println(childDeptsValue);

//       2.创建一个回收部门
            Put put = new Put(Bytes.toBytes("0_004"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    Bytes.toBytes("deleted_dept"));
            table.put(put);
//       3.将原部门的子部门保存在回收部门下
            for(int i=0;i<childDeptsId.size();i++){
//            为回收部门添加子部门信息
                Put put2 = new Put(Bytes.toBytes("0_004"));
                put2.addColumn(Bytes.toBytes("subdept"), Bytes.toBytes(childDeptsId.get(i)),
                        Bytes.toBytes(childDeptsValue.get(i)));
                table.put(put2);
//            修改子部门的分部门信息
                Put put3 = new Put(Bytes.toBytes(childDeptsId.get(i)));
                put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f_pid"),
                        Bytes.toBytes("0_004"));
                table.put(put3);
            }

            //4.删除原部门的数据
            Delete del = new Delete(Bytes.toBytes("1_003"));
            table.delete(del);


        }catch (Exception e){
            e.printStackTrace();
        }

        return 200;
    }
}

