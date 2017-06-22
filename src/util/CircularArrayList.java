package util;

import java.util.ArrayList;
import java.util.List;

public class CircularArrayList<E> extends ArrayList<E>{
    private static final long serialVersionUID = 1L;

    public CircularArrayList(List<E> list){
    	super();
    	for(E element : list){
    		this.add(element);
    	}
    }
    
    public CircularArrayList() {
    	super();
	}

	public E get(int index){
        if (index < 0)
            index = size() + index;

        else if (index >= size())
            index = index - size();

        return super.get(index);
    }
}
