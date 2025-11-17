package com.gpb.jdata.metrics;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Aspect
@Component
@Slf4j
public class ExecutionTimeAspect {
    
    @Around("execution(* com.gpb.jdata.service.*.*(..)) && " +
            "!within(com.gpb.jdata.service.CefLogFileService) && ")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        
        try {
            Object result = joinPoint.proceed();
            long executionTime = System.currentTimeMillis() - startTime;
            
            log.info("Метод {} выполнен за {} мс", 
                    joinPoint.getSignature().toShortString(), 
                    executionTime);
            
            return result;
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            log.error("Метод {} завершился с ошибкой за {} мс. Ошибка: {}", 
                    joinPoint.getSignature().toShortString(), 
                    executionTime, 
                    e.getMessage());
            throw e;
        }
    }

    @Around("execution(* com.gpb.jdata.orda.service.*.*(..)) && " +
            "!within(com.gpb.jdata.orda.service.KeycloakAuthService) && ")
    public Object logOrdaExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        
        try {
            Object result = joinPoint.proceed();
            long executionTime = System.currentTimeMillis() - startTime;
            
            log.info("Метод {} выполнен за {} мс", 
                    joinPoint.getSignature().toShortString(), 
                    executionTime);
            
            return result;
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            log.error("Метод {} завершился с ошибкой за {} мс. Ошибка: {}", 
                    joinPoint.getSignature().toShortString(), 
                    executionTime, 
                    e.getMessage());
            throw e;
        }
    }
}
