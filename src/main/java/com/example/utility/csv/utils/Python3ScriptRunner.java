package com.example.utility.csv.utils;

import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.Arrays;

public class Python3ScriptRunner {

    @Value("${python3.path}")
    private static String python3Path;

    public static Process run(String... args) throws IOException {
        StringBuilder arguments = new StringBuilder(python3Path);
        Arrays.stream(args).forEach((String arg) -> arguments.append(" ").append(arg));
        return Runtime.getRuntime().exec(arguments.toString());
    }
}
