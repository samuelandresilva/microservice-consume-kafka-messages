package com.samuel.jsonconsumer.listener;

import static java.lang.Thread.sleep;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.samuel.jsonconsumer.model.Payment;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class JsonListener {
	
	@SneakyThrows
	@KafkaListener(groupId = "create-group", topics = "payment-topic", containerFactory = "jsonContainerFactory")
	public void antiFraud(@Payload Payment payment) {
		log.info("Recebi o pagamento {}", payment.toString());
		sleep(2000);
		
		log.info("Validando fraude...");
		sleep(2000);
		
		log.info("Compra aprovada...");
		sleep(3000);
	}
	
	@SneakyThrows
	@KafkaListener(groupId = "pdf-group", topics = "payment-topic", containerFactory = "jsonContainerFactory")
	public void pdfGenerator(@Payload Payment payment) {
		log.info("Gerando PDF do produto de id {}...", payment.getIdProduct());
		sleep(3000);
	}
	
	@SneakyThrows
	@KafkaListener(groupId = "email-group", topics = "payment-topic", containerFactory = "jsonContainerFactory")
	public void sendMail() {
		log.info("Enviando e-mail de confirmação...");
	}
}
