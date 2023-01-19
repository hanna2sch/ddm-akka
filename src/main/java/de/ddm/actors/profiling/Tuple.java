package de.ddm.actors.profiling;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Tuple{
    private final int a;
    private final int b;
    boolean equals(Tuple temp){
        if (temp.getA()==this.a && temp.getB()==this.b)
            return true;
        if (temp.getB()==this.a && temp.getA()==this.b)
            return true;
        return false;
    }
}