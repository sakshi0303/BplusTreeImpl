JAR=btreelib.jar
LOGJAR=log4j-1.2.17.jar

#this is the name of the given project folder
#ASSIGNMENT=btree_project_spring_23
ASSIGNMENT=btproject

#change the ASSIGN path to the path where you have downloaded on your computer
#ASSIGN=/home/s/sa/santraa/CSE5331
ASSIGN=/home/s/sx/sx3702

#change the JDKPATH if you are not using omega.uta.edu
JDKPATH = /opt/jdk1.6.0_20
LIBPATH = $(ASSIGN)/$(ASSIGNMENT)/lib/$(JAR)
LIBPATH2 = $(ASSIGN)/$(ASSIGNMENT)/lib/$(LOGJAR)
#CLASSPATH = $(LIBPATH):$(ASSIGN)/$(ASSIGNMENT)/src
CLASSPATH = $(LIBPATH):$(LIBPATH2):$(ASSIGN)/$(ASSIGNMENT)/src
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core
