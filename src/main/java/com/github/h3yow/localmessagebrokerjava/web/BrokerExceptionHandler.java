package com.github.h3yow.localmessagebrokerjava.web;

import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.EnumMap;
import java.util.Map;

/**
 * 把业务异常映射为带明确错误码的 HTTP 响应。
 *
 * <ul>
 *   <li>404：主题/消费组不存在</li>
 *   <li>400：请求或消息本身非法、偏移量非法</li>
 *   <li>409：回退位点被拒、租约失效（调用方操作与当前状态冲突）</li>
 *   <li>503：队列瞬时写满（CAPACITY_FULL，可稍后重试）</li>
 * </ul>
 */
@RestControllerAdvice
public class BrokerExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(BrokerExceptionHandler.class);

    private static final Map<BrokerErrorCode, HttpStatus> STATUS = new EnumMap<>(BrokerErrorCode.class);

    static {
        STATUS.put(BrokerErrorCode.TOPIC_NOT_FOUND, HttpStatus.NOT_FOUND);
        STATUS.put(BrokerErrorCode.GROUP_NOT_FOUND, HttpStatus.NOT_FOUND);
        STATUS.put(BrokerErrorCode.CAPACITY_FULL, HttpStatus.SERVICE_UNAVAILABLE);
        STATUS.put(BrokerErrorCode.INVALID_MESSAGE, HttpStatus.BAD_REQUEST);
        STATUS.put(BrokerErrorCode.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
        STATUS.put(BrokerErrorCode.INVALID_OFFSET, HttpStatus.BAD_REQUEST);
        STATUS.put(BrokerErrorCode.SEEK_BACKWARD_REJECTED, HttpStatus.CONFLICT);
        STATUS.put(BrokerErrorCode.LEASE_NOT_FOUND, HttpStatus.CONFLICT);
    }

    @ExceptionHandler(BrokerException.class)
    public ResponseEntity<ErrorResponse> handleBroker(BrokerException ex) {
        HttpStatus status = STATUS.getOrDefault(ex.getCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        if (status.is5xxServerError()) {
            log.warn("请求失败: code={}, reason={}", ex.getCode(), ex.getMessage());
        }
        return ResponseEntity.status(status).body(new ErrorResponse(ex.getCode().name(), ex.getMessage()));
    }

    /** 请求体缺失或无法解析（例如 leaseId/offset 传 null）。 */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(BrokerErrorCode.INVALID_REQUEST.name(), "请求体缺失或格式非法"));
    }
}
