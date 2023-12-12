package com.example.notificationservice.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class PaymentEventStreamListener implements StreamListener<String, MapRecord<String, String, String>> {

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        Map<String, String> map = message.getValue();

        String userId = map.get("userId");
        String paymentProcessId = map.get("paymentProcessId");

        // 주문 건에 대한 메일 발송 처리
        log.info("[Order consumed] userId: " + userId + ", paymentProcessId: " + paymentProcessId);
    }
}
