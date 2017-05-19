package com.jerrypeng.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONObject;  

@WebServlet("/TeseServlet")
public class TestServlet extends HttpServlet {  
  
    private static final long serialVersionUID = 1L;  
  
    public TestServlet() {  
        super();  
    }  
  
    public void destroy() {  
        super.destroy();   
    }  
  
    public void doGet(HttpServletRequest request, HttpServletResponse response)  
            throws ServletException, IOException {  
        doPost(request, response);  
    }  
  
    public void doPost(HttpServletRequest request, HttpServletResponse response)  
            throws ServletException, IOException {  
    	 response.setContentType("text/html;charset=UTF-8");  
         BufferedReader bfr = new BufferedReader(new InputStreamReader((request.getInputStream())));
         String s = "";
         StringBuilder sb = new StringBuilder();
         while((s=bfr.readLine())!=null){
        	 sb.append(s);
         }
         PrintWriter out = response.getWriter();
         String info = new String(sb);
         out.println();
         JSONObject json = JSONObject.fromObject(info);
         String time = json.getString("time");
         File file = new CreatDirClass().CreatDir(time);
         BufferedWriter bfw = new BufferedWriter(new FileWriter(file,true));
         bfw.write(json.toString());
         bfw.flush();
         bfw.close();
         
    }  
  
    public void init() throws ServletException {  
    }  
}  