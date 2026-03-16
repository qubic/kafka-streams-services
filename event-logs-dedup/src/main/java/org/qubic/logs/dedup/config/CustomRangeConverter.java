package org.qubic.logs.dedup.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConfigurationPropertiesBinding
public class CustomRangeConverter implements Converter<String, Range<Long>> {

    @Override
    public Range<Long> convert(String source) {
        final Range<Long> range;
        if (source.contains("-")) { // range definition
            if (StringUtils.countMatches(source, "-") != 1) {
                throw new IllegalArgumentException("Invalid range format for ignored keys: " + source);
            }
            String[] parts = source.split("-");
            long start = Long.parseLong(parts[0].trim());
            long end = Long.parseLong(parts[1].trim());
            range = Range.of(start, end);
        } else { // number definition
            range = Range.of(Long.parseLong(source.trim()), Long.parseLong(source.trim()));
        }
        log.info("Crated range: {}.", range);
        return range;
    }

}
