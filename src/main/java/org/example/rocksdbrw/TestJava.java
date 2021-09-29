package org.example.rocksdbrw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class TestJava {
  public static void main(String[] args) {
    String[] a = new String[]{"a", "b", "c"};
    System.out.println(Arrays.asList(a).equals(new ArrayList<>(Arrays.asList(a))));
    System.out.println(Arrays.asList(a).equals(new ArrayList<>(Arrays.asList(a))));
  }
}
