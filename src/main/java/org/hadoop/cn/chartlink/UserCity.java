package org.hadoop.cn.chartlink;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class UserCity implements WritableComparable<UserCity>{
	
	//name的key --- name  --- city的名字 --- city的编号
	//标识
	private String userID = "";
	private String userName = "";
	private String cityID = "";
	private String cityName = "";
	private int flag = 0;
	
	public UserCity() {
	}
	
	public UserCity(String userID, String userName,String cityID ,String cityName,int flag) {
		this.userID=userID;
		this.userName=userName;this.cityID=cityID;this.cityName=cityName;this.userID=userID;
	}
	
	public UserCity(UserCity uc) {
		this.userID=uc.getUserID();
		this.userName=uc.getUserName();
		this.cityID=uc.getCityID();
		this.cityName=uc.getCityName();
		this.flag=uc.getFlag();
	}

	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getCityID() {
		return cityID;
	}
	public void setCityID(String cityID) {
		this.cityID = cityID;
	}
	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.userID);
		out.writeUTF(this.userName);
		out.writeUTF(this.cityID);
		out.writeUTF(this.cityName);
		out.writeInt(this.flag);
		
	}

	public void readFields(DataInput input) throws IOException {
		this.userID=input.readUTF();
		this.userName=input.readUTF();
		this.cityID=input.readUTF();
		this.cityName=input.readUTF();
		this.flag=input.readInt();
		
	}

	public int compareTo(UserCity arg0) {
		return 0;
	}
	
	@Override
	public String toString() {
		return "userID"+userID+",userName"+userName+",cityName"+cityName;
	}
}
