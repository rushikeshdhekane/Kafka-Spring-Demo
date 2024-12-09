package com.appsdeveloperblog.ws.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.restcontroller.CreateProductRestModel;

@Service
public class ProdctServiceImpl implements ProductService{
	
	private Logger logger = LoggerFactory.getLogger(ProdctServiceImpl.class);

	@Autowired
	KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	
	@Override
	public String createProductAsynchronus(CreateProductRestModel productRestModel) {
		
		String productId = UUID.randomUUID().toString();
		
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
				productRestModel.getTitle(), productRestModel.getPrice(),
				productRestModel.getQuantity());
		
		//CompletableFuture used for async response
		CompletableFuture<SendResult<String, ProductCreatedEvent>> future = 
				kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent);
		
		future.whenComplete((result,exception) ->{
			
			if(exception!=null) {
				logger.error("Failed to send message "+exception.getMessage());
			}
			else {
				logger.info("Message send successfully "+result.getRecordMetadata());
			}
		});
		
		logger.info("***** Returning product id *****");
		
		return productId;
	}

	@Override
	public String createProductSynchronus(CreateProductRestModel productRestModel) throws Exception {
		
		String productId = UUID.randomUUID().toString();
		
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
				productRestModel.getTitle(), productRestModel.getPrice(),
				productRestModel.getQuantity());
		
		logger.info("Before publishing a productCreatedEvent");
		
		ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
				"product-created-events-topic",
				productId,
				productCreatedEvent);
		
		record.headers().add("messageId",UUID.randomUUID().toString().getBytes());
		
		logger.info("Producer side productId :"+productId+" messageId : "+UUID.randomUUID().toString());
		
		//CompletableFuture used for async response
//		SendResult<String, ProductCreatedEvent> result = 
//				kafkaTemplate.send("product-created-event-topic", productId, productCreatedEvent).get();
//		
		
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send(record).get();
		
		logger.info("Topic Synchronus: "+result.getRecordMetadata().topic());
		logger.info("Partition Synchronus: "+result.getRecordMetadata().partition());
		logger.info("Offset Synchronus: "+result.getRecordMetadata().offset());
		
		
		logger.info("***** Returning product id *****");
		
		return productId;
	}

}
