package com.samuel.jsonconsumer.model;

import java.io.Serializable;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Payment implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private Long id;
	private Long idUser;
	private Long idProduct;
	private String cardNumber;
	
}
