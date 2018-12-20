FROM centos:7
# Maintainer
# ----------
MAINTAINER Amber Road Release Team <BLR_TA15_Release_Team@amberroad.com>
 
RUN echo "Starting Qualification Prep Service Image Build"
 
LABEL version="1.0" description="This is the Dockerfile for Qualification Prep Service image"
ARG JAR_FILE_NAME
ENV JAR_FILE_NAME=$JAR_FILE_NAME
 
# Add files required to build this image
RUN mkdir -p /opt/micro_service
COPY $JAR_FILE_NAME /opt/micro_service/
RUN mkdir -p /opt/micro_service/config
WORKDIR /opt/micro_service/
RUN yum -y install java-1.8.0-openjdk
 
# Define default command to start bash. 
CMD java ${USER_JVM_ARGS} -jar ${JAR_FILE_NAME}