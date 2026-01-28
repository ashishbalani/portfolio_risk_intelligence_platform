package com.portfolio.risk.refdata.controller;

import com.portfolio.risk.refdata.entity.InstrumentEntity;
import com.portfolio.risk.refdata.service.InstrumentService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/instruments")
public class InstrumentController {
    private final InstrumentService instrumentService;

    public InstrumentController(InstrumentService instrumentService) {
        this.instrumentService = instrumentService;
    }

    @GetMapping
    public List<InstrumentEntity> search(@RequestParam(value = "query", required = false) String query) {
        return instrumentService.search(query);
    }

    @GetMapping("/{instrumentId}")
    public InstrumentEntity get(@PathVariable String instrumentId) {
        return instrumentService.get(instrumentId)
                .orElseThrow(() -> new IllegalArgumentException("Instrument not found"));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.OK)
    public InstrumentEntity upsert(@Valid @RequestBody InstrumentUpsertRequest request) {
        InstrumentEntity entity = new InstrumentEntity();
        entity.setInstrumentId(request.instrumentId());
        entity.setSymbol(request.symbol());
        entity.setName(request.name());
        entity.setType(request.type());
        entity.setCurrency(request.currency());
        entity.setStatus(request.status());
        return instrumentService.upsert(entity);
    }

    public record InstrumentUpsertRequest(
            @NotBlank String instrumentId,
            @NotBlank String symbol,
            @NotBlank String name,
            @NotBlank String type,
            @NotBlank String currency,
            @NotBlank String status
    ) {
    }
}
