package jgh.elastic;

public class User {
    public int getGrade() {
        return grade;
    }
    public void setGrade(int grade) {
        this.grade = grade;
    }
    public int getGclass() {
        return gclass;
    }
    public void setGclass(int gclass) {
        this.gclass = gclass;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    User(int grade,int gclass,String name){
	this.grade=grade;
	this.gclass=gclass;
	this.name=name;
    }
    private int grade;
    private int gclass;
    private String name;
}
