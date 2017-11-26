package com.abdul.java8.samples.flatmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FlatMapExample {

	 public static void main(String[] args) {

	        Student obj1 = new Student();
	        obj1.setName("mkyong");
	        obj1.addBook("Java 8 in Action");
	        obj1.addBook("Spring Boot in Action");
	        obj1.addBook("Effective Java (2nd Edition)");

	        Student obj2 = new Student();
	        obj2.setName("zilap");
	        obj2.addBook("Learning Python, 5th Edition");
	        obj2.addBook("Effective Java (2nd Edition)");

	        List<Student> list = new ArrayList<>();
	        list.add(obj1);
	        list.add(obj2);

	        List<String> collect =
	                list.stream()
	                        .map(x -> x.getBook())      //Stream<Set<String>>
	                        .flatMap(x -> x.stream())   //Stream<String>
	                        .distinct()
	                        .collect(Collectors.toList());

	        collect.forEach(x -> System.out.println(x));
	        
	        
	        
	        
	        int[] intArray = {1, 2, 3, 4, 5, 6};

	        //1. Stream<int[]>
	        Stream<int[]> streamArray = Stream.of(intArray);

	        //2. Stream<int[]> -> flatMap -> IntStream
	        IntStream intStream = streamArray.flatMapToInt(x -> Arrays.stream(x));

	        intStream.forEach(x -> System.out.println(x));
	        
	        
	        Stream<int[]> streamArray2 = Stream.of(intArray);
	        
	        
	        //2. Stream<int[]> -> flatMap -> IntStream
	        Stream<Object> intStream2 = streamArray2.map(x -> Arrays.stream(x));

	    //    intStream2.forEach(x -> System.out.println(x));
	        
	        
	        
	        
	        
	        
	        List<String> lst = Arrays.asList("STACK","OOOVER");
	      List<String[]>  list2 =   lst.stream().map(w->w.split("")).distinct().collect(Collectors.toList());
	      list2.stream().forEach(x -> System.out.println(x)); 
	      
	      
	      List<Stream<String>>  list3 =   lst.stream().map(w->w.split(""))
	    		                                .map(Arrays::stream)
	    		                               .distinct().collect(Collectors.toList());
	      list3.stream().forEach(x -> System.out.println(x)); 
	      
	      
	      
	      List<String> list4 =   lst.stream().map(w->w.split(""))
                  .flatMap(Arrays::stream)
                 .distinct().collect(Collectors.toList());
list4.stream().forEach(x -> System.out.println(x)); 
	    }

}
