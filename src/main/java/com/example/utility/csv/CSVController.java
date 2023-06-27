package com.example.utility.csv;

import com.example.utility.csv.utils.CSVUtil;
import com.example.utility.csv.utils.SQLiteUtil;
import com.example.utility.csv.utils.ZipUtil;
import com.google.gson.Gson;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.lingala.zip4j.exception.ZipException;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller()
public class CSVController {

    @Value("${upload.dir}")
    private String uploadDir;

    private final String oldFileName = "oldFile.csv";
    private final String newFileName = "newFile.csv";

    private final Gson gson = new Gson();

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/preview")
    public String preview(Model model, @RequestParam String id) throws IOException {
        Path oldCSVFilePath = Paths.get(uploadDir, id, oldFileName);
        Path newCSVFilePath = Paths.get(uploadDir, id, newFileName);
        final int numOfRows = 10;
        ArrayList<String> oldTableHeaders = CSVUtil.getHeaders(oldCSVFilePath);
        ArrayList<String> newTableHeaders = CSVUtil.getHeaders(newCSVFilePath);
        ArrayList<ArrayList<String>> oldTableRows = CSVUtil.getRowsWithoutHeaders(oldCSVFilePath, numOfRows);
        ArrayList<ArrayList<String>> newTableRows = CSVUtil.getRowsWithoutHeaders(newCSVFilePath, numOfRows);
        model.addAttribute("oldTableHeaders", oldTableHeaders);
        model.addAttribute("newTableHeaders", newTableHeaders);
        model.addAttribute("oldTableRows", oldTableRows);
        model.addAttribute("newTableRows", newTableRows);
        return "preview";
    }

    @GetMapping("/process")
    public String process() {
        return "process";
    }

    @CrossOrigin
    @GetMapping("/process/events")
    public SseEmitter processEvents(@RequestParam String id, @RequestParam String oldPrimaryKeyIdx, @RequestParam String newPrimaryKeyIdx) {
        final Path oldCSVFilePath = Paths.get(uploadDir, id, oldFileName);
        final Path newCSVFilePath = Paths.get(uploadDir, id, newFileName);
        final SseEmitter emitter = new SseEmitter();

        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(() -> {
            try {
                ArrayList<String> oldTableHeaders = CSVUtil.getHeaders(oldCSVFilePath);
                ArrayList<String> newTableHeaders = CSVUtil.getHeaders(newCSVFilePath);
                processCSVs(oldCSVFilePath, newCSVFilePath,
                        oldTableHeaders.get(Integer.parseInt(oldPrimaryKeyIdx)),
                        newTableHeaders.get(Integer.parseInt(newPrimaryKeyIdx)),
                        emitter);
                Path targetDir = Paths.get(uploadDir, id, "output");
                ZipUtil.zip(targetDir);
            } catch (IOException | SQLException e) {
                e.printStackTrace();
                emitter.completeWithError(e);
                throw new RuntimeException(e);
            }
            finally {
                emitter.complete();
            }
        });

        return emitter;
    }

    @GetMapping(value = "/download", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public void getResult(HttpServletRequest request, HttpServletResponse response) throws IOException, URISyntaxException {
        String id = request.getParameter("id");
        response.setHeader("Content-Disposition", "attachment; filename=" + id + ".zip");
        Path targetFile = Paths.get(uploadDir, id, "output.zip");

        ServletOutputStream oStream = response.getOutputStream();

        BufferedInputStream iStream = new BufferedInputStream(new FileInputStream(targetFile.toString()));
        byte[] buffer = new byte[1024 * 1024];
        int bytesRead = -1;

        while ((bytesRead = iStream.read(buffer)) != -1) {
            oStream.write(buffer, 0, bytesRead);
        }

        iStream.close();
        oStream.close();
    }

    private void sentEmitterTextResponse(SseEmitter emitter, String messageKey, String message) throws IOException {
        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        String formattedDate = myDateObj.format(myFormatObj);

        HashMap<String, String> response = new HashMap<>();
        response.put(messageKey, formattedDate + " > " + message);
        emitter.send(gson.toJson(response), MediaType.TEXT_PLAIN);
    }

    private void processCSVs(Path oldCSVFilePath, Path newCSVFilePath, String oldPrimaryKey, String newPrimaryKey, SseEmitter emitter) throws IOException, SQLException {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        try {
    //        Validate CSV Files
            sentEmitterTextResponse(emitter, "start", "Starting Operation");
            sentEmitterTextResponse(emitter, "message", "Validating CSV Files");

//            if (CSVUtil.doesContainEmptyHeader(oldCSVFilePath)
//                || CSVUtil.doesContainEmptyHeader(newCSVFilePath)) {
//                response = new HashMap<>();
//                response.put("error", "Validation Error: CSV contains empty headers");
//                emitter.send(gson.toJson(response), MediaType.TEXT_PLAIN);
//                return;
//            }

            sentEmitterTextResponse(emitter, "message", "Validation Completed Successfully");
            sentEmitterTextResponse(emitter, "message", "Pre-processing CSV Files");

//            CSVUtil.checkAndProcessRows(oldCSVFilePath);
//            CSVUtil.checkAndProcessRows(newCSVFilePath);

            sentEmitterTextResponse(emitter, "message", "Pre-processing Files Completed Successfully");

    //        Create Schema From CSV Files
            sentEmitterTextResponse(emitter, "message", "Creating SQLite Schema From CSV Files");

            SQLiteUtil.createSchemaFromOldCSV(oldCSVFilePath);
            SQLiteUtil.createSchemaFromNewCSV(newCSVFilePath);

            sentEmitterTextResponse(emitter, "message", "Schema Created Successfully");
            sentEmitterTextResponse(emitter, "step", "1");

    //        Create Tables From Schemas
            sentEmitterTextResponse(emitter, "message", "Creating Tables from Schemas");

            SQLiteUtil.createOldTableFromSchemaFile(oldCSVFilePath);
            SQLiteUtil.createNewTableFromSchemaFile(newCSVFilePath);

            sentEmitterTextResponse(emitter, "message", "Tables Created Successfully");

    //        Import Data From CSV Files
            sentEmitterTextResponse(emitter, "message", "Importing Data From CSV Files");

            SQLiteUtil.importDataFromOldCSV(oldCSVFilePath);
            SQLiteUtil.importDataFromNewCSV(newCSVFilePath);

            sentEmitterTextResponse(emitter, "message", "Data Imported Successfully");
            sentEmitterTextResponse(emitter, "step", "2");

    //        Check Differences
            sentEmitterTextResponse(emitter, "message", "Checking Differences");

            SQLiteUtil.getRowsDeleted(oldCSVFilePath, oldPrimaryKey, newPrimaryKey);
            SQLiteUtil.getRowsAdded(oldCSVFilePath, oldPrimaryKey, newPrimaryKey);
            SQLiteUtil.getRowsUpdated(oldCSVFilePath, oldPrimaryKey, newPrimaryKey);

            sentEmitterTextResponse(emitter, "message", "Checking Completed Successfully");
            sentEmitterTextResponse(emitter, "step", "3");

    //        Export To Excel
            sentEmitterTextResponse(emitter, "stop", "Operation Completed");
            sentEmitterTextResponse(emitter, "message", "Exporting Result To Excel Files");

            SQLiteUtil.exportToExcel(oldCSVFilePath, newCSVFilePath);

            sentEmitterTextResponse(emitter, "complete", "Exported Successfully");

        }
        catch (Exception e) {
            e.printStackTrace();
            sentEmitterTextResponse(emitter, "error", e.getMessage());
        }
        finally {
            stopWatch.stop();
            sentEmitterTextResponse(emitter, "message", "Operation Time: " + stopWatch.getTotalTimeSeconds() + " SECONDS");
            sentEmitterTextResponse(emitter, "closeConnection", "Close Connection");
        }

    }
}
