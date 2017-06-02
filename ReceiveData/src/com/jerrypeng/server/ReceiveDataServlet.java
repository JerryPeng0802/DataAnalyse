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



/**
 * Servlet implementation class ReceiveDataServlet
 */
@WebServlet("/ReceiveDataServlet")
public class ReceiveDataServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ReceiveDataServlet() {
        super();
        // TODO Auto-generated constructor stub
    }
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
		// TODO Auto-generated method stub
		response.setContentType("text/html;charset=UTF-8");  
        BufferedReader bfr = new BufferedReader(new InputStreamReader((request.getInputStream())));
        String s = "";
        StringBuilder sb = new StringBuilder();
        while((s=bfr.readLine())!=null){
       	 sb.append(s);
        }
        String info1 = new String(sb);
        //String info = request.getParameter("data");
        String info = info1.substring(5);
        JSONObject json = JSONObject.fromObject(info);
        String time = json.getString("time");
        File file = new CreatDirClass().CreatDir(time);
        BufferedWriter bfw = new BufferedWriter(new FileWriter(file,true));
        bfw.write(json.toString());
        bfw.newLine();
        bfw.flush();
        bfw.close();
        
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
		
	}

}
