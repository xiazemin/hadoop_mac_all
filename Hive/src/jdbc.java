import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class jdbc {
//	 private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
//	   
//	   public static void main(String[] args) throws SQLException {
//	      // Register driver and create driver instance
//		   Class.forName(driverName);
//	      // get connection
//	      
//	      Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
//	      Statement stmt = con.createStatement();
//	      
//	      stmt.executeQuery("CREATE DATABASE userdb_jdbc");
//	      System.out.println("Database userdb created successfully.");
//	      
//	      con.close();
//	   }
private static String driverName ="org.apache.hive.jdbc.HiveDriver";
public static void main(String[] args) throws SQLException {
  try {
      Class.forName(driverName);
  } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
  }

  Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/", "APP", "");
  Statement stmt = con.createStatement();
  String tableName = "userdb";
  stmt.execute("drop table if exists " + tableName);
  stmt.execute("create table " + tableName + 
                               " (key int, value string)");
  System.out.println("Create table success!");
  // show tables
  String sql = "show tables '" + tableName + "'";
  System.out.println("Running: " + sql);
  ResultSet res = stmt.executeQuery(sql);
  if (res.next()) {
      System.out.println(res.getString(1));
  }

  // describe table
  sql = "describe " + tableName;
  System.out.println("Running: " + sql);
  res = stmt.executeQuery(sql);
  while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
  }


  sql = "select * from " + tableName;
  res = stmt.executeQuery(sql);
  while (res.next()) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t"
                                         + res.getString(2));
  }

  sql = "select count(1) from " + tableName;
  System.out.println("Running: " + sql);
  res = stmt.executeQuery(sql);
  while (res.next()) {
      System.out.println(res.getString(1));
  }
}
}
