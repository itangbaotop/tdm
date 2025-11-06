package top.itangbao.tdm.core.model;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "tdm_task")
@Data
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Enumerated(EnumType.STRING) // 将枚举按字符串存储
    @Column(nullable = false, length = 50)
    private TaskStatus status;

    // 任务的原始数据文件在数据湖中的路径
    private String rawDataUri;



    // 关系：一个任务属于一个项目
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false)
    private Project project;
}