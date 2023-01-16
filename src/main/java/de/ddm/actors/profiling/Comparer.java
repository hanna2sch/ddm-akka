package de.ddm.actors.profiling;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Comparer {
    private int fileid;
    private int compare_fileid;
    private int colid;
    private int compare_colid;
}
