package com.gpb.jdata.orda.service;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.mapper.ViewLineageRequestBuilder;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.utils.diff.ViewDiffContainer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class ViewService {
    @Value("${ord.api.baseUrl}")
    private String ordaApiUrl;

    @Value("${ord.api.max-connections:5}")
    private int maxConnections;

    private final OrdProperties ordProperties;

    private final ViewLineageRequestBuilder requestBuilder;

    private final ViewDiffContainer diffContainer;

    public void handleViewLineage() {
        for (ViewDTO view : diffContainer.getUpdated()) {
            requestBuilder.buildEdgesForView(
                ordProperties.getBaseUrl(),
                ordProperties.getPrefixFqn(),
                view
            );
        }

        ExecutorService executor = Executors.newFixedThreadPool(maxConnections);

        try {
            List<Callable<Void>> tasks = diffContainer.getUpdated().stream()
                .<Callable<Void>>map(view -> () -> {
                    try {
                        requestBuilder.buildEdgesForView(
                            ordProperties.getBaseUrl(),
                            ordProperties.getPrefixFqn(),
                            view
                        );
                    } catch (Exception e) {
                        log.error("Ошибка при обработке Lineage view={}: {}", view.getViewName(), e.getMessage());
                    }
                    return null;
                })
                .toList();
            executor.invokeAll(tasks, 20, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Ошибка при обработке Lineage для представлений: {}", e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
