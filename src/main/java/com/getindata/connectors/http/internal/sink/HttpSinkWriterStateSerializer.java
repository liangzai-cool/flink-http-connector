package com.getindata.connectors.http.internal.sink;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;

/**
 * An implementation of {@link AsyncSinkWriterStateSerializer} for {@link HttpSinkInternal} and its
 * {@link HttpSinkWriter}.
 */
@Slf4j
public class HttpSinkWriterStateSerializer
    extends AsyncSinkWriterStateSerializer<HttpSinkRequestEntry> {

    @Override
    protected void serializeRequestToStream(HttpSinkRequestEntry s, DataOutputStream out)
        throws IOException {
        log.info("debug, serializeRequestToStream");
        out.writeUTF(s.method);
        out.write(s.element);
    }

    @Override
    protected HttpSinkRequestEntry deserializeRequestFromStream(long requestSize,
        DataInputStream in) throws IOException {
        log.info("debug, deserializeRequestFromStream");
        String method = in.readUTF();
        byte[] bytes = new byte[(int) requestSize];
        in.read(bytes);
        return new HttpSinkRequestEntry(method, bytes);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BufferedRequestState<HttpSinkRequestEntry> obj) throws IOException {
        log.info("debug, serialize");
        return super.serialize(obj);
    }

    @Override
    public BufferedRequestState<HttpSinkRequestEntry> deserialize(int version, byte[] serialized) throws IOException {
        log.info("debug, deserialize");
        return super.deserialize(version, serialized);
    }
}
