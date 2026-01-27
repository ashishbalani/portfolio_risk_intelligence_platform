package com.portfolio.risk.processingstreams.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyingTest {

    @Test
    void positionKeyFormats() {
        assertEquals("P|B|I", Keying.positionKey("P", "B", "I"));
        assertEquals("||", Keying.positionKey(null, null, null));
    }

    @Test
    void exposureKeyFormats() {
        assertEquals("P|DIM|BUCKET", Keying.exposureKey("P", "DIM", "BUCKET"));
        assertEquals("||", Keying.exposureKey(null, null, null));
    }
}
