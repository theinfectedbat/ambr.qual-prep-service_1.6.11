package com.ambr.gtm.fta.qps.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class })
@ComponentScan({"com.ambr"})
public class Application 
{
	public static void main(String[] theArgs)
		throws Exception
	{
		SpringApplication.run(Application.class, theArgs);
    }
}
