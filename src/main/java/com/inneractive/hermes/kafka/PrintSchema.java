package com.inneractive.hermes.kafka;

import com.inneractive.hermes.model.JoinFullEvent;

/**
 * Created by Richard Grossman on 2017/06/05.
 */
public class PrintSchema {
    public static void main(String[] argv) {
        System.out.print(JoinFullEvent.getClassSchema().toString(true));
    }
}
