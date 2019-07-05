package cn.kgc;
import	java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class Java_Hbase {
    public void createtable() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("peoples");
            if (!admin.isTableAvailable(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor("name"));
                hTableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
                hTableDescriptor.addFamily(new HColumnDescriptor("personalinfo"));
                admin.createTable(hTableDescriptor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void deletetable() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("peoples");
            if (admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void listtable() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            HTableDescriptor[] HTableDescriptor = admin.listTables();
            for (int i = 0; i < HTableDescriptor.length; i++) {
                System.out.println(HTableDescriptor[i].getNameAsString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void instertdata() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("peoples"));
            String[][] people = {
                    {"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
                    {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
                    {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
                    {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
                    {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
                    {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}};
            for (int i = 0; i < people.length; i++) {
                Put put = new Put(Bytes.toBytes(people[i][0]));
                put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
                put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
                put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
                put.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"), Bytes.toBytes(people[i][4]));
                put.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));

                table.put(put);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (table != null) {
                    table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void getdata() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        Table table = null;
        Result result = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf("peoples");
            admin = connection.getAdmin();
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(TableName.valueOf("peoples"));
                Get get = new Get(Bytes.toBytes("1"));
                result = table.get(get);
                byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
                String firstName = Bytes.toString(firstname);
                System.out.println(firstName);
            } else {
                System.out.println("表不存在");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (table != null) {
                    table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void deletedata() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("peoples"));
            List<Delete> deletes = new ArrayList<>();
            for (int rowKey = 1; rowKey <= 5; rowKey++) {
                deletes.add(new Delete(Bytes.toBytes(rowKey + "")));
            }
            table.delete(deletes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    public void scantabledata() {
        Configuration configuration = HBaseConfiguration.create();
        //需要把hbase-site.xml和core-site.xml复制到resources文件夹中，其中节点的ip要配置到windows的hosts文件中
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        Connection connection = null;
        Admin admin = null;
        Table table = null;
        ResultScanner resultscanner = null;
        Result result = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("peoples"));
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"));
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"));
            scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                    Bytes.toBytes("personalinfo"),
                    Bytes.toBytes("gender"),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("M"));
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                    Bytes.toBytes("personalinfo"),
                    Bytes.toBytes("age"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,
                    Bytes.toBytes("18"));

           FilterList filterList = new FilterList();
           filterList.addFilter(filter1);
           filterList.addFilter(filter2);

            scan.setFilter(filterList);
            resultscanner = table.getScanner(scan);
            result = resultscanner.next();

            while (result != null) {
                byte[] firstNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
                byte[] lastNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
                byte[] emailValue = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
                byte[] genderValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
                byte[] ageValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

                String firstName = Bytes.toString(firstNameValue);
                String lastName = Bytes.toString(lastNameValue);
                String email = Bytes.toString(emailValue);
                String gender = Bytes.toString(genderValue);
                String age = Bytes.toString(ageValue);
                System.out.println("姓名：" + firstName + " " + lastName + "邮件：" + email + "性别：" + gender + "年龄：" + age);
                result = resultscanner.next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (table != null) {
                    table.close();
                }

                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
