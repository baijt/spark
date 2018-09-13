package com.sprak.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

public class HystrixTest2  extends HystrixCommand<String>{
	


	private String name;
	
	protected HystrixTest2(String name) {
		super(HystrixCommandGroupKey.Factory.asKey("ExampleGroup"));
		this.name = name;
	}

	@Override
	protected String run() throws Exception {
		return "Hello " + name + "! thread: " + Thread.currentThread().getName();
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
		HystrixTest2  hystrixTest = new HystrixTest2("Synchronous-hystrix");
		String s = hystrixTest.execute();
		System.err.println(s);
		
		hystrixTest = new HystrixTest2("Asynchronous-hystrix");
		Future<String> future = hystrixTest.queue();
		s = future.get(100, TimeUnit.MILLISECONDS);
		System.err.println(s);
		
	}

}
