package project.bdt;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
public class Main {
	static String COLUMN_FAMILY_FIELDS = "column_family_one";
	static String TABLE = "table_stock";
	private static final String cf1c1 = "column_one";
	private static final String cf1c2 = "column_second";
	private static final String cf1c3 = "column_third";
	private static final String cf1c4 = "column_four";

	private static void addRow(Row row, Table table) {
		try {
			Put p;
			p = new Put(Bytes.toBytes(row.getString(0)));
			p.addColumn(Main.COLUMN_FAMILY_FIELDS.getBytes(), cf1c1.getBytes(), Bytes.toBytes(row.getString(1)));
			p.addColumn(Main.COLUMN_FAMILY_FIELDS.getBytes(), cf1c2.getBytes(), Bytes.toBytes(row.getString(2)));
			p.addColumn(Main.COLUMN_FAMILY_FIELDS.getBytes(), cf1c3.getBytes(), Bytes.toBytes(row.getString(3)));

			p.addColumn(Main.COLUMN_FAMILY_FIELDS.getBytes(), cf1c4.getBytes(), Bytes.toBytes(row.getString(4)));

			table.put(p);
		}
		catch (Exception e) {
    	 e.printStackTrace(); }
	}
	private static void createTable(Admin admin, Connection connection) throws ClassNotFoundException, SQLException, IOException {

    	HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE));
		tableDescriptor.addFamily(new HColumnDescriptor(Main.COLUMN_FAMILY_FIELDS));

		if (!admin.tableExists(tableDescriptor.getTableName())) {
			admin.createTable(tableDescriptor);
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			try (java.sql.Connection con 
					= java.sql.DriverManager.getConnection("jdbc:hive2://localhost:10000", "cloudera", "cloudera")) {
				Statement stmt = con.createStatement();
			    stmt.execute("drop table " + tableDescriptor.getTableName());
			    stmt.execute(
			    		"CREATE EXTERNAL TABLE " + tableDescriptor.getTableName() + " ("
			    			+ "id string,"
    			    		+ "column_one float,"
    			    		+ "column_second float,"
    			    		+ "column_third float,"
    			    		+ "column_four string) "
			    		+ "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' "
			    		+ "WITH SERDEPROPERTIES ('hbase.columns.mapping' = "
    			    		+ "':key#b,"
    			    		+ "column_family_one:column_one,"
    			    		+ "column_family_one:column_second,"
    			    		+ "column_family_one:column_third,"
    			    		+ "column_family_one:column_four')");
			}
		}
	}
	
	private static void sparkSQLCalc(Connection connection) throws IOException {
		Table myTable = connection.getTable(TableName.valueOf(TABLE));
    	
    	SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Project");
    	JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
    	SQLContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);
    	DataFrame df = sqlContext.read()
        .format("com.databricks.spark.csv")
        .option("header", "false") 
        .option("mode", "DROPMALFORMED")
        .load("MyLogger.csv");
    	df.registerTempTable("table_stock");

    	DataFrame newDF = sqlContext.sql("SELECT * FROM table_stock WHERE C1 >= 200");
    	DataFrame maxDF = sqlContext.sql("SELECT MAX(C1) FROM table_stock GROUP BY 1");

    	df.javaRDD().collect()
    	  .forEach(row -> {
    		  addRow(row, myTable);
    	  });
    	
    	// Displays the content of the DataFrame to stdout
    	newDF.show();
    	maxDF.show();

	}
    public static void main( String[] args ) throws ClassNotFoundException, SQLException {
    	Configuration configuration = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(configuration);
				Admin admin = connection.getAdmin();) {
			createTable(admin, connection);
			sparkSQLCalc(connection);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
}
