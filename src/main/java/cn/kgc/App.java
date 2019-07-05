package cn.kgc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Java_Hbase java_hbase = new Java_Hbase();
//            java_hbase.createtable();
////           java_hbase.deletetable();
////          java_hbase.listtable();
//            java_hbase.instertdata();
//             java_hbase.getdata();
//            java_hbase.deletedata();
        java_hbase.scantabledata();
        }
}
