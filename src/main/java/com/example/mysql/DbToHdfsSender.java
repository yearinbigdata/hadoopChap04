package com.example.mysql;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DbToHdfsSender {

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		String cls = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/world";
//		String url = "jdbc:mysql://ec2:3306/world";
		String user = "jspbook";
		String password = "1234";

		Class.forName(cls); // 클래스 로딩

		Connection con = DriverManager.getConnection(url, user, password);
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		sendCity(con, hdfs);
		sendCountry(con, hdfs);
		sendCountryLanguage(con, hdfs);
		
		hdfs.close();
		con.close();

		System.out.println("END...");
	}

	static void sendCity(Connection con, FileSystem hdfs) throws SQLException, IOException {
		PreparedStatement pstmt = con.prepareStatement("select * from city");
		ResultSet rs = pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/city.txt"));
		PrintWriter writer = new PrintWriter(out);

		while(rs.next()){
			System.out.println(".");
			writer.printf("%10s %-30s %10s %-30s %10s", rs.getString("id"),
												rs.getString("name"),
												rs.getString("countryCode"),
												rs.getString("district"),
												rs.getString("population"));
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
	}

	static void sendCountry(Connection con, FileSystem hdfs) throws SQLException, IOException{
		PreparedStatement pstmt = con.prepareStatement("select * from country");
		ResultSet rs = pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/country.txt"));
		PrintWriter writer = new PrintWriter(out);

		while(rs.next()){
			System.out.println(".");
			writer.printf("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
							, rs.getString("code"),
							rs.getString("name"),
							rs.getString("continent"),
							rs.getString("region"),
							rs.getString("surfaceArea"),
							rs.getString("indepYear"),
							rs.getString("population"),
							rs.getString("lifeExpectancy"),
							rs.getString("gnp"),
							rs.getString("gnpOld"),
							rs.getString("localName"),
							rs.getString("governmentForm"),
							rs.getString("headOfState"),
							rs.getString("capital"),
							rs.getString("code2"));
			
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
	}

	static void sendCountryLanguage(Connection con, FileSystem hdfs) throws SQLException, IOException{
		PreparedStatement pstmt = con.prepareStatement("select * from countrylanguage");
		ResultSet rs = pstmt.executeQuery();
		
		FSDataOutputStream out = hdfs.create(new Path("/home/java/world/countrylanguage.txt"));
		PrintWriter writer = new PrintWriter(out);

		while(rs.next()){
			System.out.println(".");
			writer.printf("%s, %s, %s, %s", rs.getString("countryCode"),
												rs.getString("language"),
												rs.getString("isOfficial"),
												rs.getString("percentage"));
			writer.println();
		}
		
		writer.close();
		out.close();
		rs.close();
		pstmt.close();
	}

}
