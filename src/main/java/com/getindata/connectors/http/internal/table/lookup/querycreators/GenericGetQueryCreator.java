package com.getindata.connectors.http.internal.table.lookup.querycreators;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.utils.uri.NameValuePair;
import com.getindata.connectors.http.internal.utils.uri.URLEncodedUtils;

/**
 * A {@link LookupQueryCreator} that builds an "ordinary" GET query, i.e. adds
 * <code>joinColumn1=value1&amp;joinColumn2=value2&amp;...</code> to the URI of the endpoint.
 */
@Slf4j
public class GenericGetQueryCreator implements LookupQueryCreator {

    private final LookupRow lookupRow;

    public GenericGetQueryCreator(LookupRow lookupRow) {
        this.lookupRow = lookupRow;
    }

    @Override
    public LookupQueryInfo createLookupQuery(RowData lookupDataRow) {
        log.info("debug, createLookupQuery, lookupDataRow: {}", JSON.toJSONString(lookupDataRow));
        Collection<LookupArg> lookupArgs = lookupRow.convertToLookupArgs(lookupDataRow);

        String lookupQuery =
            URLEncodedUtils.format(
                lookupArgs.stream()
                        .map(arg -> new NameValuePair(arg.getArgName(), arg.getArgValue()))
                        .collect(Collectors.toList()),
                StandardCharsets.UTF_8);

        return new LookupQueryInfo(lookupQuery);
    }
}
