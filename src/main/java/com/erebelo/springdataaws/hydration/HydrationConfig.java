package com.erebelo.springdataaws.hydration;

import com.erebelo.springdataaws.hydration.domain.dto.RecordDto;
import com.erebelo.springdataaws.hydration.service.HydrationService;
import com.erebelo.springdataaws.hydration.service.contract.ContractAdvisorService;
import com.erebelo.springdataaws.hydration.service.contract.ContractFirmService;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HydrationConfig {

    @Bean
    @SuppressWarnings("squid:S1452")
    public List<HydrationService<? extends RecordDto>> hydrationPipeline(ContractAdvisorService contractAdvisorService,
            ContractFirmService contractFirmService) {
        return List.of(contractAdvisorService, contractFirmService);
    }
}
