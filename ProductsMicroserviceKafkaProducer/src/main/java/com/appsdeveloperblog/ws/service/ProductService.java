package com.appsdeveloperblog.ws.service;

import com.appsdeveloperblog.ws.restcontroller.CreateProductRestModel;

public interface ProductService {

	String createProductAsynchronus(CreateProductRestModel createProductRestModel);
	String createProductSynchronus(CreateProductRestModel createProductRestModel) throws Exception;
}
