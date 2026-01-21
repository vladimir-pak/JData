package com.gpb.jdata.orda.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gpb.jdata.orda.client.OrdaClient;
import com.gpb.jdata.orda.dto.ViewDTO;
import com.gpb.jdata.orda.dto.lineage.AddLineageRequest;
import com.gpb.jdata.orda.mapper.ViewLineageRequestBuilder;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.orda.repository.TableRepository;
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
    private final OrdaClient ordaClient;

    private final ViewLineageRequestBuilder requestBuilder;
    private final String LINEAGE_URL = "/lineage";
    private final TableRepository tableRepository;

    private final ViewDiffContainer diffContainer;

    public void handleViewLineage() {
        List<AddLineageRequest> requests = new ArrayList<>();

        for (ViewDTO view : diffContainer.getUpdated()) {
            // Lineage request
            List<AddLineageRequest> viewRequests = requestBuilder.buildEdgesForView(
                ordProperties.getBaseUrl(),
                ordProperties.getPrefixFqn(),
                view
            );
            requests.addAll(viewRequests);
        }

        String url = ordaApiUrl + LINEAGE_URL;

        ExecutorService executor = Executors.newFixedThreadPool(maxConnections);

        try {
            List<Callable<Void>> tasks = requests.stream()
                .<Callable<Void>>map(req -> () -> {
                    try {
                        ordaClient.sendPutRequest(url, req, String.format(
                            "Добавление или обновление view lineage %s", 
                            req.getEdge().getToEntity().getId()));
                    } catch (Exception e) {
                        log.error("Ошибка при обработке Lineage view={}: {}", 
                            req.getEdge().getToEntity().getId(), e.getMessage());
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

    public void putLineageByOid(Long oid) {
        ViewDTO view = tableRepository.findViewByOid(oid);

        List<AddLineageRequest> requests = requestBuilder.buildEdgesForView(
            ordProperties.getBaseUrl(),
            ordProperties.getPrefixFqn(),
            view
        );

        String url = ordaApiUrl + LINEAGE_URL;

        ExecutorService executor = Executors.newFixedThreadPool(maxConnections);

        try {
            List<Callable<Void>> tasks = requests.stream()
                .<Callable<Void>>map(req -> () -> {
                    try {
                        ordaClient.sendPutRequest(url, req, String.format(
                            "Добавление или обновление view lineage %s", 
                            req.getEdge().getToEntity().getId()));
                    } catch (Exception e) {
                        log.error("Ошибка при обработке Lineage view={}: {}", 
                            req.getEdge().getToEntity().getId(), e.getMessage());
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
