package basetool;
import java.util.*;

public class myCmp implements Comparator{

//	    public int compare(String o1,String o2) {
//	    	
//	    	if(o1.compareTo(o2)<0)
//	           return 1;
//	       else
//	           return 0;
//	       }

		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			
			String s1=(String)o1;
			String s2=(String)o2;
			
			if(s1.compareTo(s2)<0)
		           return 1;
		       else
		           return 0;
		}
	
}
