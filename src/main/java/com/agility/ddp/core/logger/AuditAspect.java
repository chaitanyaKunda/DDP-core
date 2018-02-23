/************************************************************************
 * Copyright (c) 2014, All rights reserved.
 * 
 * AuditAspect.java 
 * 
 * 
 *  
 * 
 * @author 		:	Kiru
 * @created		:	24-Feb-2014
 * @version 	:	
 * @modified by	:	Administrator
 * @modified on	:	24-Feb-2014 : 13:37:46
 * 
 ************************************************************************/
package com.agility.ddp.core.logger;

import com.agility.ddp.core.components.ApplicationProperties;
import com.agility.ddp.data.domain.DdpAuditTxn;
import com.agility.ddp.data.domain.DdpAuditTxnService;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.context.SecurityContextHolder;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Auto-generated Javadoc
/**
 * The Class AuditAspect.
 */
@Aspect
@Component
@Configurable
//@PropertySource({"file:///E:/DDPConfig/ddp.properties"})
public class AuditAspect 
{
	
	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(AuditAspect.class);
	
	/** The ddp audit txn service. */
	@Autowired
    DdpAuditTxnService ddpAuditTxnService;
	
	@Autowired
	private ApplicationProperties env;

	//@Before("execution(public String com.agility.ddp.view.web.*Controller.*(..)) && @annotation(audit)")
	/**
	 * Before.
	 *
	 * @param joinPoint the join point
	 */
	@Before(value = "@annotation(audit)", argNames = "joinPoint, audit")
	public void before(JoinPoint joinPoint, AuditLog audit) 
	{	
		logger.debug("Method AuditAspect.before invoked.");
				
		String strCallerClass = joinPoint.getSignature().getDeclaringType().getName();
		String strCallerMethod = joinPoint.getSignature().getName();
		String strUserName = getUserName(strCallerClass,joinPoint.getArgs(),strCallerMethod);
		
		String strArgs = getArgs(joinPoint.getArgs());
		
		String strEventName = (!audit.eventName().equals(""))?audit.eventName():env.getProperty("event_name_"+strCallerClass, "UNKNOWN");
		String strEventSource = (!audit.eventSource().equals(""))?audit.eventSource():strCallerMethod;
		String strApplicationName = (!audit.applicationName().equals(""))?audit.applicationName():env.getProperty("app_name_"+strCallerClass, "UNKNOWN");
		String strMsg = "[Initiation] - "+audit.message();
		
		if(audit.dbAuditBefore())
		{
			insertAuditLog(strEventName, strEventSource, strCallerClass, strApplicationName, strMsg, "", strUserName);
		}
		
		logger.debug("Method AuditAspect.before Completed.");
	}
	
	/**
	 * After returning.
	 *
	 * @param joinPoint the join point
	 * @param audit the audit
	 * @param returnParameter the return parameter
	 */
	@AfterReturning(value = "@annotation(audit)", returning = "returnParameter", argNames = "joinPoint, audit, returnParameter")
	public void afterReturning(JoinPoint joinPoint, AuditLog audit, Object returnParameter) 
	{
		logger.debug("Method AuditAspect.afterReturning invoked.");
		
		String strCallerClass = joinPoint.getSignature().getDeclaringType().getName();
		String strCallerMethod = joinPoint.getSignature().getName();
		String strUserName = getUserName(strCallerClass,joinPoint.getArgs(),strCallerMethod);
		
		String strArgs = getArgs(joinPoint.getArgs());
		String outparam = getReturnParams(returnParameter);
		
		String strEventName = (!audit.eventName().equals(""))?audit.eventName():env.getProperty("event_name_"+strCallerClass, "UNKNOWN");
		String strEventSource = (!audit.eventSource().equals(""))?audit.eventSource():strCallerMethod;
		String strApplicationName = (!audit.applicationName().equals(""))?audit.applicationName():env.getProperty("app_name_"+strCallerClass, "UNKNOWN");
		String strMsg = "[Success] - "+audit.message();
		
		if(audit.dbAuditAfter())
		{
			insertAuditLog(strEventName, strEventSource, strCallerClass, strApplicationName, strMsg, outparam, strUserName);
		}
				
		logger.debug("Method AuditAspect.afterReturning Completed.");
	}
	
	/**
	 * After throwing.
	 *
	 * @param joinPoint the join point
	 * @param ex the ex
	 */
	@AfterThrowing(value = "@annotation(audit)", throwing = "ex", argNames = "joinPoint, audit, ex")
	public void afterThrowing(JoinPoint joinPoint, AuditLog audit, Throwable ex) 
	{
		logger.debug("Method AuditAspect.afterThrowing invoked.");
		
		String strCallerClass = joinPoint.getSignature().getDeclaringType().getName();
		String strCallerMethod = joinPoint.getSignature().getName();
		String strUserName = getUserName(strCallerClass,joinPoint.getArgs(),strCallerMethod);
		
		String strArgs = getArgs(joinPoint.getArgs());
		
		String strEventName = (!audit.eventName().equals(""))?audit.eventName():env.getProperty("event_name_"+strCallerClass, "UNKNOWN");
		String strEventSource = (!audit.eventSource().equals(""))?audit.eventSource():strCallerMethod;
		String strApplicationName = (!audit.applicationName().equals(""))?audit.applicationName():env.getProperty("app_name_"+strCallerClass, "UNKNOWN");
		String strMsg = "[Exception] - "+audit.message();
		
		if(audit.dbAuditException())
		{
			insertAuditLog(strEventName, strEventSource, strCallerClass, strApplicationName, strMsg, ex.getMessage(), strUserName);
		}
		
		logger.debug("Method AuditAspect.afterThrowing Completed.");
	}
	
	/**
	 * Insert audit log.
	 *
	 * @param atxEventName the atx event name
	 * @param atxEventSource the atx event source
	 * @param atxObjectName the atx object name
	 * @param atxApplicationName the atx application name
	 * @param atxDetail the atx detail
	 * @param outparam the outparam
	 * @param atxCreatedBy the atx created by
	 */
	private void insertAuditLog(String atxEventName, String atxEventSource, String atxObjectName, String atxApplicationName, String atxDetail, String outparam, String atxCreatedBy)
	{
		try
		{
			DdpAuditTxn ddpAuditTxn= new DdpAuditTxn();
            
            ddpAuditTxn.setAtxEventName(atxEventName.length() > 16 ?atxEventName.substring(0, 16):atxEventName);
            ddpAuditTxn.setAtxEventSource(atxEventSource.length() > 45 ?atxEventSource.substring(0, 45):atxEventSource);
            ddpAuditTxn.setAtxObjectName(atxObjectName.length() > 120 ?atxObjectName.substring((atxObjectName.length()-120)):atxObjectName);
            ddpAuditTxn.setAtxApplicationName(atxApplicationName.length() > 45 ?atxApplicationName.substring(0, 45):atxApplicationName);
            ddpAuditTxn.setAtxDetail((atxDetail+outparam).length() > 256?(atxDetail+outparam).substring(0, 256):(atxDetail+outparam));
            ddpAuditTxn.setAtxCreatedBy(atxCreatedBy.length() > 45 ?atxCreatedBy.substring(0, 45):atxCreatedBy);
            ddpAuditTxn.setAtxCreatedDate(Calendar.getInstance());
                        
            ddpAuditTxnService.saveDdpAuditTxn(ddpAuditTxn);
            
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			logger.error("Error occured while inserting audit log {}",ex.toString());
		}
	}
	
	private String getUserName(String strCallerClass,Object[] objArgs, String strCallerMethod)
	{
		String strUserName = "";
		try
		{
			User user = (User)SecurityContextHolder.getContext().getAuthentication().getPrincipal();
			strUserName = user.getUsername();
		}
		catch(Exception ex)
		{
			logger.warn("Exception while accessing UserName : {} - This could be login attempt, so no active user exists",ex.getMessage());
			
			if( strCallerClass != null && strCallerClass.equals("com.agility.ddp.view.auth.CustomJdbcDaoImpl") && strCallerMethod.equals("loadUserByUsername"))
			{
				strUserName = objArgs[0].toString();
			}
			
		}
		
		return strUserName;
	}
	
	private String getArgs(Object[] objArgs)
	{
		StringBuffer buffer = new StringBuffer();
		for (Object object : objArgs) 
		{
			if (null != object && "" != object) 
			{
				buffer.append(object.toString());
				buffer.append(", ");
			}
		}
		
		return buffer.toString();
	}
	
	private String getReturnParams(Object returnParameter)
	{
		String outparam = null;
		 
		if (null != returnParameter && (returnParameter instanceof Map || returnParameter instanceof List)) 
		{
			outparam = "";
		} 
		else if (null != returnParameter) 
		{
			outparam = returnParameter.toString();
		}
		
		return outparam;
	}
}
