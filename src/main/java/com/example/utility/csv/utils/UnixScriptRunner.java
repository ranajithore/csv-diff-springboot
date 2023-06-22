package com.example.utility.csv.utils;

import java.io.IOException;
import java.util.Arrays;

public class UnixScriptRunner {

    public static Process run(String... args) throws IOException {
        StringBuilder arguments = new StringBuilder();
        Arrays.stream(args).forEach((String arg) -> arguments.append(" ").append(arg));
        String[] commands = {"/bin/bash", "-c", arguments.toString()};
        return Runtime.getRuntime().exec(commands);
    }
}
