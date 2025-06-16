package com.example.demo.config;

import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@Component
public class KafkaConfigLoader {
    public KafkaResourcesWrapper loadConfig(String filePath) throws Exception {
        LoaderOptions loaderOptions = new LoaderOptions();
        Constructor constructor = new Constructor(KafkaResourcesWrapper.class, loaderOptions);
        Yaml yaml = new Yaml(constructor);

        try (InputStream in = getClass().getClassLoader().getResourceAsStream(filePath)) {
            if (in == null) {
                throw new IllegalArgumentException("File not found in classpath: " + filePath);
            }
            return yaml.load(in);
        }
    }
}