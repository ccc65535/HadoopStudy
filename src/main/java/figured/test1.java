package figured;

import java.util.*;

public class test1 {
	public static void main(String []args){
		/*String s="9000111000000000000";
		long a=1L;
		Long b=2L;
		System.out.println(s.getBytes().length);
		System.out.println(s.length());
		System.out.println(Long.parseLong(s));*/
		List<Integer> list1=new ArrayList<Integer>();
		list1.add(1);
		list1.add(2);
		list1.add(3);
		System.out.println(list1.toString());
		
		List<Integer> list2=new ArrayList<Integer>(list1);
		list2.remove(0);
		list2.remove(1);
		System.out.println(list2.toString());
		System.out.println(list1.toString());
		
	}
}
