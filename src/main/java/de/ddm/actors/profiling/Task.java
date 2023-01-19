package de.ddm.actors.profiling;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class Task {
    private int id;
    private int id_comp;
    private List<List<String>> batch;
    private List<List<String>> batch_comp;
}
