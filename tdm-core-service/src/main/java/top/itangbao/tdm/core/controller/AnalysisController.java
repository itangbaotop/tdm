package top.itangbao.tdm.core.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import top.itangbao.tdm.core.model.AnalysisResult;
import top.itangbao.tdm.core.repository.AnalysisResultRepository;
import top.itangbao.tdm.core.service.SparkOrchestrationService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class AnalysisController {

    private final SparkOrchestrationService sparkOrchestrationService;
    private final AnalysisResultRepository analysisResultRepository;

    @PostMapping("/tasks/{taskId}/run-analysis")
    public ResponseEntity<String> runAnalysis(@PathVariable Long taskId, 
                                            @RequestBody AnalysisRequest request) {
        String resultId = UUID.randomUUID().toString();
        
        sparkOrchestrationService.submitAnalysisJob(taskId, request.getQuery(), resultId);
        
        return ResponseEntity.ok(resultId);
    }

    @GetMapping("/analysis-results/{resultId}")
    public ResponseEntity<AnalysisResult> getAnalysisResult(@PathVariable String resultId) {
        return analysisResultRepository.findById(resultId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    public static class AnalysisRequest {
        private String query;
        
        public String getQuery() { return query; }
        public void setQuery(String query) { this.query = query; }
    }
}