package org.qubic.logs.dedup.config;

import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CustomRangeConverterTest {

    private final CustomRangeConverter converter = new CustomRangeConverter();

    @ParameterizedTest
    @CsvSource({
            "5, 5, 5",
            " 10 , 10, 10",
            "0, 0, 0"
    })
    void shouldConvertSingleNumber(String input, long expectedStart, long expectedEnd) {
        Range<Long> result = converter.convert(input);
        assertThat(result).isNotNull();
        assertThat(result.getMinimum()).isEqualTo(expectedStart);
        assertThat(result.getMaximum()).isEqualTo(expectedEnd);
    }

    @ParameterizedTest
    @CsvSource({
            "1-3, 1, 3",
            " 5 - 10 , 5, 10",
            "100-100, 100, 100"
    })
    void shouldConvertRange(String input, long expectedStart, long expectedEnd) {
        Range<Long> result = converter.convert(input);
        assertThat(result).isNotNull();
        assertThat(result.getMinimum()).isEqualTo(expectedStart);
        assertThat(result.getMaximum()).isEqualTo(expectedEnd);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "1-2-3",
            "a-b",
            "1-",
            "-1",
            " ",
            ""
    })
    void shouldThrowExceptionOnInvalidFormat(String input) {
        assertThatThrownBy(() -> converter.convert(input))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldHandleLargeNumbers() {
        String input = "10000000000-20000000000";
        Range<Long> result = converter.convert(input);
        assertThat(result).isNotNull();
        assertThat(result.getMinimum()).isEqualTo(10000000000L);
        assertThat(result.getMaximum()).isEqualTo(20000000000L);
    }
}
