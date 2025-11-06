package top.itangbao.tdm.core.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name = "analysis_results")
@Data
public class AnalysisResult {

    @Id
    private String id;

    @Column(columnDefinition = "TEXT")
    private String result;

    @Column(nullable = false)
    private String status;

    @Column(name = "created_time")
    private LocalDateTime createdTime;
}