package de.ddm.actors.profiling;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class Column{
    private final int fileId;
    private final int colId;
    private final List<String> colData;
}
