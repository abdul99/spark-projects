package com.bigdata.spark.streaming;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SocketWriter {
	
	 public static void main(String[] args) throws Exception {
	        System.out.println("Begin");
	        /**
	         * Create socket server
	         */
	        ServerSocket server = new ServerSocket(9999);
	        //open socket
	        Socket socket = server.accept();
	 
	        DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
	 
	        System.out.println("Start writing data. Enter close when finish");
	        Scanner sc = new Scanner(System.in);
	        String str;
	        /**
	         * Read content from scanner and write to socket.
	         */
	        while (!(str = sc.nextLine()).equals("close")) {
	            outputStream.writeUTF(str);
	        }
	        //close connection now.
	        server.close();
	    }

}
