package top.itangbao.tdm.core.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import top.itangbao.tdm.core.model.Task;

public interface TaskRepository extends JpaRepository<Task, Long> {
}

