java -Xmx4096m -Xms4096m -agentlib:jdwp=transport=dt_socket,server=y,address=8001,suspend=n -jar qtx_ambr.qual-prep-service-1.0.0.jar --proc=QTX --spring.config.additional-location=qtx.properties