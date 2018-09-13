package com.sprak.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;


public class HystrixTest  extends HystrixCommand<String>{
	


	private String name;
	
	protected HystrixTest(String name) {
		super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("HelloWorldGroup")) //在没有线程池分配的情况下 使用组名进行分配线程池。同一个组名分配的是同一个线程池
				.andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(500))
				.andCommandKey(HystrixCommandKey.Factory.asKey("commandHelloWorld2"))
				.andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("HelloWorldPool"))
				);
		this.name = name;
	}

	@Override
	protected String run() throws Exception {
		TimeUnit.MICROSECONDS.sleep(400000);
		return "Hello " + name + "! thread: " + Thread.currentThread().getName();
	}
	
	@Override
	protected String getFallback() {
		return "execute Falled";
	}
	
	
	public static void main(String[] args) {
		HystrixTest commandHelloWorld2 = new HystrixTest("test-Fallback");
		String s = commandHelloWorld2.execute();
		System.out.println( s);
		
		/**
		 * 注意：
		 * 1.除了HystrixBadRequestException异常之外，所有从run()方法抛出的异常都算作失败，
		               并触发降级getFallback()和断路器逻辑。
		    
           2.HystrixBadRequestException用在非法参数或非系统故障异常等不应触发回退逻辑的场景。
           
           3.每个CommandKey代表一个依赖抽象,相同的依赖要使用相同的CommandKey名称。
                                      依赖隔离的根本就是对相同CommandKey的依赖做隔离.
                                      
           4.CommandGroup是每个命令最少配置的必选参数，
                                      在不指定ThreadPoolKey的情况下，字面值用于对不同依赖的线程池/信号区分       
                                      
           5.当对同一业务依赖做隔离时使用CommandGroup做区分,
           	但是对同一依赖的不同远程调用如(一个是redis 一个是http),
           	可以使用HystrixThreadPoolKey做隔离区分.
           	最然在业务上都是相同的组，但是需要在资源上做隔离时，
           	可以使用HystrixThreadPoolKey区分.   
           	
           6.以下四种情况将触发getFallback调用：
             1.)run()方法抛出非HystrixBadRequestException异常。
             2.)run()方法调用超时
             3.)熔断器开启拦截调用
             4.)线程池/队列/信号量是是否跑满
                                        实现getFallback()后,执行命令时遇到以上4中情况将被fallback接管,
                                        不会抛出异常或其他。                      	                                                                 
		 */
		
	}

}
