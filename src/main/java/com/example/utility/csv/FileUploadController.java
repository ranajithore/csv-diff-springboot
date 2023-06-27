package com.example.utility.csv;

import com.example.utility.csv.utils.ZipUtil;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.UUID;

@RestController
public class FileUploadController {

    @Value("${upload.dir}")
    private String uploadDir;

    private static final Gson gson = new Gson();

    @CrossOrigin
    @RequestMapping(value = "/upload", method = RequestMethod.POST, consumes = "multipart/form-data")
    @ResponseBody
    public ResponseEntity<String> upload(@RequestParam("oldFile") MultipartFile oldFile, @RequestParam("newFile") MultipartFile newFile) throws IOException {
        UUID uuid = UUID.randomUUID();
        if (!(saveFile(uuid, oldFile) && saveFile(uuid, newFile))) {
            return ResponseEntity.status(401).body("unsupported files");
        }
        HashMap<String, String> response = new HashMap<>();
        response.put("id", uuid.toString());
        response.put("message", "uploaded");
        return ResponseEntity.ok(gson.toJson(response));
    }

    private boolean saveFile(UUID uuid, MultipartFile file) throws IOException {
        Path destination = Paths.get(uploadDir, uuid.toString());
        Files.createDirectories(destination);
        Path zipFilePath = destination.resolve(file.getName() + ".zip");
        Files.createFile(zipFilePath);
        file.transferTo(zipFilePath);
        if (ZipUtil.numberOfCSVFilesInZip(zipFilePath) != 1) {
            return false;
        }
        ZipUtil.unzip(zipFilePath, file.getName() + ".csv");
        return true;
    }
}
