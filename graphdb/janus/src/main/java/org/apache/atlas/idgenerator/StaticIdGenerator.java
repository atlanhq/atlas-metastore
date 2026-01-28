package org.apache.atlas.idgenerator;

import java.util.UUID;

public class StaticIdGenerator implements IdGenerator {
    @Override
    public String nextId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
