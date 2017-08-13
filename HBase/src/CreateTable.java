import java.io.IOException;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.sun.jersey.json.impl.writer.JsonEncoder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;


import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class CreateTable {
      
   public static void main(String[] args) throws IOException {
	   try {
   // Instantiating configuration class
   Configuration con = HBaseConfiguration.create();
   con.set("hbase.zookeeper.quorum", "localhost"); 
   con.set("hbase.zookeeper.property.clientPort", "2183");
   System.out.println(con.toString());
   // Instantiating HbaseAdmin class
   
   HBaseAdmin admin = new HBaseAdmin(con);
   System.out.println(con.toString());
   System.out.println(admin.toString());
   // Instantiating table descriptor class
   if(admin.tableExists("emp_j")){
	   // Getting all the list of tables using HBaseAdmin object
	   HTableDescriptor[] tableDescriptors =admin.listTables();

	   // printing all the table names.
	   for (int i=0; i<tableDescriptors.length;i++ ){
	      System.out.println(tableDescriptors[i].getNameAsString());
	   }
	   
	   // Instantiating HTable class
	      HTable hTable = new HTable(con, "emp_j");
	   // Instantiating Put class
	      // accepts a row name.
	      Put p = new Put(Bytes.toBytes("row1")); 

	      // adding values using add() method
	      // accepts column family name, qualifier/row name ,value
	      p.add(Bytes.toBytes("personal"),
	      Bytes.toBytes("name"),Bytes.toBytes("raju"));

	      p.add(Bytes.toBytes("personal"),
	      Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

	      p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
	      Bytes.toBytes("manager"));

	      p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
	      Bytes.toBytes("50000"));
	      
	      // Saving the put Instance to the HTable.
	      hTable.put(p);
	      System.out.println("data inserted");
	      
	      
	      // Instantiating Get class
	      Get g = new Get(Bytes.toBytes("row1"));

	      // Reading the data
	      Result result = hTable.get(g);

	      // Reading values from Result class object
	      byte [] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));

	      byte [] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));

	      // Printing the values
	      String name = Bytes.toString(value);
	      String city = Bytes.toString(value1);
	      
	      System.out.println("name: " + name + " city: " + city);
	      
	      
	      
	      
	      // Instantiating the Scan class
	      Scan scan = new Scan();

	      // Scanning the required columns
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

	      // Getting the scan result
	      ResultScanner scanner = hTable.getScanner(scan);

	      // Reading values from scan result
	      for (Result results = scanner.next(); results != null; results = scanner.next())

	      System.out.println("Found row : " + results);
	      //closing the scanner
	      scanner.close();
	      
	      
	      // closing HTable
	      hTable.close();
	      
   }else {
	
   HTableDescriptor tableDescriptor = new
   HTableDescriptor(TableName.valueOf("emp_j"));

   // Adding column families to table descriptor
   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
   tableDescriptor.addFamily(new HColumnDescriptor("professional"));
   // Execute the table through admin
  
   admin.createTable(tableDescriptor);
   System.out.println(" Table created ");
   }
   admin.close();

   } catch (Exception e) {
       e.printStackTrace();
   }
 
   }
  }