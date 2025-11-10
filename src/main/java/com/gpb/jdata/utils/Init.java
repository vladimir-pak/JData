package com.gpb.jdata.utils;

import com.gpb.jdata.service.*;

import lombok.AllArgsConstructor;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import java.util.concurrent.*;

@Component
@AllArgsConstructor
public class Init implements CommandLineRunner {
    // private static final Logger logger = LoggerFactory.getLogger(Init.class);
    private final PGAttrdefService pgAttrdefService;
    private final PGAttributeService pgAttributeService;
    private final PGClassService pgClassService;
    private final PGConstraintService pgConstraintService;
    private final PGDescriptionService pgDescriptionService;
    private final PGNamespaceService pgNamespaceService;
    // private PGPartitionedTableService pgPartitionedTableService;
    private final PGTypeService pgTypeService;
    private final PGViewsService pgViewsService;
    private final PGDatabaseService pgDatabaseService;
    private final PGPartitionRuleService pgPartitionRuleService;
    private final PGPartitionService pgPartitionService;
    private final PGSequenceService pgSequenceService;
    // private DatabaseConfig databaseConfig;

    @Override
    public void run(String... args) {
        ExecutorService executorService = Executors.newFixedThreadPool(11);
        for (int i = 0; i < 12; i++) {
            final int threadNumber = i;
            executorService.submit(() -> {
                try {
                    runMethod(threadNumber);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    // TODO implement rest func
    // из-за того, что sql проходится сразу по нескольким таблицам
    // существует проблема итогового ожидания самого "долгого" потока
    // таблица pg_attribute_rep содержит свыше 1кк записей, которые
    // по изначальной задаче необходимо было просто сравнить и записать в лог-таблицу что именно изменилось
    // ее сравнение работает долго - остальные таблицы намного быстрее
    private void runMethod(int methodNumber) throws InterruptedException {
        switch (methodNumber) {
            case 0 -> {
                while (true){
                    pgAttrdefService.synchronize(); Thread.sleep(60000);
                }
            }
            case 1 -> {
                while (true) {
                    pgViewsService.synchronize();  Thread.sleep(70000);
                }
            }
            case 2 -> {
                while (true) {
                pgDescriptionService.synchronize();  Thread.sleep(300000);
                }
            }
            case 3 -> {
                while (true) {
                    pgNamespaceService.synchronize();  Thread.sleep(300000);}
            }
            case 4 -> {
                while (true) {
                    pgConstraintService.synchronize();  Thread.sleep(300000);
                }
            }
            case 5 -> {
                while (true) {
                    pgClassService.synchronize();  Thread.sleep(300000);
                }
            }
            case 6 -> {
                while (true) {
                    pgTypeService.synchronize(); Thread.sleep(300000);
                }
            }
            case 7 -> {
                while (true) {
                    pgAttributeService.synchronize(); Thread.sleep(100000);
                }
            }
            case 8 -> {
                while (true) {
                    pgDatabaseService.synchronize(); Thread.sleep(60000);
                }
            }
            case 9 -> {
                while (true) {
                    pgPartitionRuleService.synchronize(); Thread.sleep(60000);
                }
            }
            case 10 -> {
                while (true) {
                    pgPartitionService.synchronize(); Thread.sleep(60000);
                }
            }
            case 11 -> {
                while (true) {
                    pgSequenceService.synchronize(); Thread.sleep(60000);
                }
            }
        }
    }

    private void initMaster() throws InterruptedException {
        pgNamespaceService.synchronize();
        pgConstraintService.synchronize();
        // pgPartitionedTableService.synchronize();
        pgViewsService.synchronize();
        pgDescriptionService.synchronize();
        pgTypeService.synchronize();
        pgClassService.synchronize();
        pgAttrdefService.synchronize();
    }
}
