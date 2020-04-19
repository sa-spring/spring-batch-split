package com.example.springbatchsplit.config;

import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job parallelStepsJob() {

        Flow masterFlow = new FlowBuilder<Flow>("masterFlow").start(taskletStep("step1")).build();

        Flow flowJob2 = new FlowBuilder<Flow>("flow2").start(taskletStep("step2")).build();
        Flow flowJob3 = new FlowBuilder<Flow>("flow3").start(taskletStep("step3")).build();
        Flow flowJob4 = new FlowBuilder<Flow>("flow4").start(taskletStep("step4")).build();

        Flow slaveFlow = new FlowBuilder<Flow>("splitflow").split(new SimpleAsyncTaskExecutor())
                .add(flowJob2, flowJob3, flowJob4).build();

        return (jobBuilderFactory.get("splitFlowJob").incrementer(new RunIdIncrementer()).start(masterFlow)
                .next(slaveFlow).build()).build();

    }

    private TaskletStep taskletStep(String step) {
        return stepBuilderFactory.get(step).tasklet((contribution, chunkContext) -> {
            IntStream.range(1, 100).forEach(token -> logger.info("Step:" + step + " token:" + token));
            return RepeatStatus.FINISHED;
        }).build();

    }

}
