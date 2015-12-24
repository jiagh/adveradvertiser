FROM index.alauda.cn/tusimple/hadoop:demo_v1
MAINTAINER BT.2015-11-23

#项目名称
ENV FILE_NAME adnet-da-report

#工作目录
WORKDIR /usr/local/$FILE_NAME

#初始化环境变量
ENV JAVA_HOME /usr/java/jdk1.7.0_75
ENV PATH $PATH:$JAVA_HOME/bin
ENV MAVEN_HOME /usr/local/apache-maven-3.3.3
ENV PATH $PATH:$MAVEN_HOME/bin

#取得代码 
COPY . /usr/local/$FILE_NAME
#HADOOP 配置文件
RUN cp -rf /usr/local/$FILE_NAME/docker/core-site.xml /usr/local/hadoop/etc/hadoop/
RUN cp -rf /usr/local/$FILE_NAME/docker/hdfs-site.xml /usr/local/hadoop/etc/hadoop/
RUN cp -rf /usr/local/$FILE_NAME/docker/yarn-site.xml /usr/local/hadoop/etc/hadoop/

#SPARK 配置文件
RUN cp -rf /usr/local/$FILE_NAME/docker/spark-defaults.conf /usr/local/spark/conf/
RUN cp -rf /usr/local/$FILE_NAME/docker/spark-env.sh /usr/local/spark/conf/

#配置MAVEN
RUN cp -rf /usr/local/$FILE_NAME/docker/settings.xml /usr/local/apache-maven-3.3.3/conf/

#代码配置文件 
RUN cp -rf /usr/local/$FILE_NAME/src/main/resources/config/demo_config.properties /usr/local/$FILE_NAME/src/main/resources/config/config.properties

#切换目录 
RUN cd /usr/local/$FILE_NAME/

#编译
RUN mvn package

#执行权限
RUN chmod 777 /usr/local/$FILE_NAME/docker/job_start.sh

CMD ["/usr/local/adnet-da-report/docker/job_start.sh"]

