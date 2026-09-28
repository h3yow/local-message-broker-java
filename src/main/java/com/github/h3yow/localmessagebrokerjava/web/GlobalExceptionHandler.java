package com.github.h3yow.localmessagebrokerjava.web;

import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.exception.ErrorCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把业务异常映射为带 errorCode 的 JSON 响应，调用方可以据 errorCode 区分失败原因：
 * <ul>
 *     <li>404：主题 / 消费组不存在</li>
 *     <li>409：重复创建、重复订阅、回退位点被拒、消息已死信等冲突</li>
 *     <li>422：消息非法、无效投递（如确认未在途的 offset）</li>
 *     <li>503：队列写满（瞬时背压，可稍后重试）</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BrokerException.class)
    public ResponseEntity<Map<String, Object>> handleBroker(BrokerException ex) {
        return ResponseEntity.status(statusFor(ex.getErrorCode()))
                .body(Map.of("errorCode", ex.getErrorCode().name(),
                        "message", String.valueOf(ex.getMessage())));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("errorCode", ErrorCode.INVALID_MESSAGE.name(),
                        "message", "malformed JSON request body"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleOther(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("errorCode", ErrorCode.INTERNAL_ERROR.name(),
                        "message", String.valueOf(ex.getMessage())));
    }

    private HttpStatus statusFor(ErrorCode code) {
        return switch (code) {
            case TOPIC_NOT_FOUND, GROUP_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case TOPIC_ALREADY_EXISTS, GROUP_ALREADY_SUBSCRIBED, SEEK_REJECTED,
                 MESSAGE_DEAD_LETTERED, DUPLICATE_KEY -> HttpStatus.CONFLICT;
            case INVALID_MESSAGE, INVALID_DELIVERY -> HttpStatus.UNPROCESSABLE_ENTITY;
            case QUEUE_FULL, RETRY_LIMIT_REACHED -> HttpStatus.SERVICE_UNAVAILABLE;
            case INTERNAL_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
