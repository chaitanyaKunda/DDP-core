/************************************************************************
 * Copyright (c) 2014, All rights reserved.
 * 
 * AuditLog.java 
 * 
 * 
 *  
 * 
 * @author 		:	Kiru
 * @created		:	24-Feb-2014
 * @version 	:	
 * @modified by	:	Administrator
 * @modified on	:	24-Feb-2014 : 13:51:03
 * 
 ************************************************************************/
package com.agility.ddp.core.logger;

import java.lang.annotation.*;

import org.springframework.stereotype.Component;

/**
 * The Interface AuditLog.
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
@Component
public @interface AuditLog {
	String value() default "";
	String eventName() default "";
	String eventSource() default "";
	String objectName() default "";
	String applicationName() default "";
	String message() default "";
	boolean dbAuditBefore() default false;
	boolean dbAuditAfter() default true;
	boolean dbAuditException() default true;
}
