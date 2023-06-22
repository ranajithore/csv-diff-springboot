package com.example.utility.csv.utils;

import ch.qos.logback.core.rolling.helper.FileNamePattern;
import com.google.common.io.Files;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import org.springframework.util.FileSystemUtils;

import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class ZipUtil {

    public static void zip(Path targetDir) throws ZipException {
        final String zipFileName = targetDir.getFileName() + ".zip";
        final Path zipFilePath = Paths.get(targetDir.getParent().toString(), zipFileName);
        new ZipFile(zipFilePath.toString()).addFolder(targetDir.toFile());
    }

    public static void unzip(Path zipFilePath, String fileName) throws IOException {
        Path tempDestination = java.nio.file.Files.createDirectories(zipFilePath.getParent().resolve("temp"));
        Path destination = zipFilePath.getParent();
        ZipFile zipFile = new ZipFile(zipFilePath.toString());
        zipFile.extractAll(tempDestination.toString());
        ArrayList<FileHeader> zipFileHeaders = zipFile.getFileHeaders()
                .stream()
                .filter((FileHeader fileHeader) ->
                        !fileHeader.isDirectory() &&
                        !fileHeader.getFileName().startsWith("__") &&
                        Files.getFileExtension(fileHeader.getFileName()).equals("csv"))
                .collect(Collectors.toCollection(ArrayList::new));
        String tempFileName = zipFileHeaders.get(0).getFileName();
        Path tempFile = java.nio.file.Files.createFile(tempDestination.resolve(tempFileName));
        Path finalFile = java.nio.file.Files.createFile(destination.resolve(fileName));
        java.nio.file.Files.move(tempFile, finalFile);
    }

    public static int numberOfCSVFilesInZip(Path zipFilePath) throws ZipException {
        ZipFile zipFile = new ZipFile(zipFilePath.toString());
        ArrayList<FileHeader> zipFileHeaders = zipFile.getFileHeaders()
                .stream()
                .filter((FileHeader fileHeader) ->
                        !fileHeader.isDirectory() &&
                                !fileHeader.getFileName().startsWith("__") &&
                        Files.getFileExtension(fileHeader.getFileName()).equals("csv"))
                .collect(Collectors.toCollection(ArrayList::new));
        return zipFileHeaders.size();
    }

    public static boolean isZipContainCSVWithSameName(Path zipFilePath) throws ZipException {
        ZipFile zipFile = new ZipFile(zipFilePath.toString());
        String zipFileNameWithoutExt = Files.getNameWithoutExtension(zipFilePath.getFileName().toString());
        ArrayList<FileHeader> fileHeaders =  (ArrayList<FileHeader>) zipFile.getFileHeaders();
        for (FileHeader fileHeader : fileHeaders) {
            if (fileHeader.isDirectory()) {
                return Files.getNameWithoutExtension(fileHeader.getFileName()).equals(zipFileNameWithoutExt);
            }
        }
        return false;
    }
}