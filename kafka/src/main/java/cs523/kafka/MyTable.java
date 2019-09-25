package cs523.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class MyTable
{

    private static final String TABLE_NAME = "user";
    private static final String CF_DEFAULT = "personal_details";

    public static void create(String args) throws IOException
    {

        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin())
        {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
            table.addFamily(new HColumnDescriptor("prof_details"));

            System.out.print("Creating table.... ");

            if (admin.tableExists(table.getTableName()))
            {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);

            System.out.println(" Done!");
        }
    }
}