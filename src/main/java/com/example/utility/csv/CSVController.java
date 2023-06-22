package com.example.utility.csv;

import com.example.utility.csv.utils.CSVUtil;
import com.example.utility.csv.utils.SQLiteUtil;
import com.example.utility.csv.utils.ZipUtil;
import net.lingala.zip4j.exception.ZipException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller()
public class CSVController {

    @Value("${upload.dir}")
    private String uploadDir;

//    private final Path oldCSVFilePath = Paths.get(uploadDir, "old.csv");
//    private final Path newCSVFilePath = Paths.get(uploadDir, "new.csv");

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/preview")
    public String preview(Model model) throws IOException {
        Path oldCSVFilePath = Paths.get(uploadDir, "Data-1.csv");
        Path newCSVFilePath = Paths.get(uploadDir, "Data-2.csv");
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

    @GetMapping("/process/events")
    public SseEmitter processEvents() {
        final SseEmitter emitter = new SseEmitter();
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(() -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    emitter.send(i + " - ", MediaType.TEXT_PLAIN);

                    Thread.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                    emitter.completeWithError(e);
                    return;
                }
            }
            emitter.complete();
        });

        return emitter;
    }

    private void processCSVs(Path oldCSVFilePath, Path newCSVFilePath) throws IOException, SQLException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        SQLiteUtil.createSchemaFromOldCSV(oldCSVFilePath);
        SQLiteUtil.createOldTableFromSchemaFile(oldCSVFilePath);
        SQLiteUtil.importDataFromOldCSV(oldCSVFilePath);

        SQLiteUtil.createSchemaFromNewCSV(newCSVFilePath);
        SQLiteUtil.createNewTableFromSchemaFile(newCSVFilePath);
        SQLiteUtil.importDataFromNewCSV(newCSVFilePath);

        SQLiteUtil.getRowsDeleted(oldCSVFilePath, "ID", "ID");
        SQLiteUtil.getRowsAdded(oldCSVFilePath, "ID", "ID");
        SQLiteUtil.getRowsUpdated(oldCSVFilePath, "ID", "ID");

        SQLiteUtil.exportToExcel(oldCSVFilePath, newCSVFilePath);

        stopWatch.stop();
        System.out.println("OPERATION TIME: " + stopWatch.getTotalTimeSeconds() + " SECONDS");
    }
}
