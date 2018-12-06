java -Xms1024m -Xmx4096m -jar qps_ambr.qual-prep-service-1.0.0.jar --proc=QPS --spring.config.additional-location=qps.properties \
-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=8181 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false
