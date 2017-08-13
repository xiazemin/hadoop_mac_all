import java.io.IOException;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.sun.jersey.json.impl.writer.JsonEncoder;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;

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
   HTableDescriptor tableDescriptor = new
   HTableDescriptor(TableName.valueOf("emp_j"));

   // Adding column families to table descriptor
   tableDescriptor.addFamily(new HColumnDescriptor("personal"));
   tableDescriptor.addFamily(new HColumnDescriptor("professional"));
   // Execute the table through admin
   admin.createTable(tableDescriptor);
   System.out.println(" Table created ");
   admin.close();

   } catch (Exception e) {
       e.printStackTrace();
   }
 
   }
  }