/* Copyright 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.ac.uct.cs.mobiperfsimulator.measurements;

import android.util.Base64;
import android.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.stomp.StompSession;
import za.ac.uct.cs.mobiperfsimulator.Constants;
import za.ac.uct.cs.mobiperfsimulator.service.BeanUtil;
import za.ac.uct.cs.mobiperfsimulator.service.WebSocketService;
import za.ac.uct.cs.mobiperfsimulator.util.MeasurementJsonConvertor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Callable class that performs download throughput test using HTTP get
 */

public class HttpTask extends MeasurementTask {

    private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);

    private StompSession wsSession;

    // Type name for internal use
    public static final String TYPE = "http";
    // Human readable name for the task
    public static final String DESCRIPTOR = "HTTP";
    /* TODO(Wenjie): Depending on state machine configuration of cell tower's radio,
     * the size to find the 'real' bandwidth of the phone may be network dependent.
     */
    // The maximum number of bytes we will read from requested URL. Set to 1Mb.
    public static final long MAX_HTTP_RESPONSE_SIZE = 1024 * 1024;
    // The size of the response body we will report to the service.
    // If the response is larger than MAX_BODY_SIZE_TO_UPLOAD bytes, we will
    // only report the first MAX_BODY_SIZE_TO_UPLOAD bytes of the body.
    public static final int MAX_BODY_SIZE_TO_UPLOAD = 1024;
    // The buffer size we use to read from the HTTP response stream
    public static final int READ_BUFFER_SIZE = 1024;
    // Not used by the HTTP protocol. Just in case we do not receive a status line from the response
    public static final int DEFAULT_STATUS_CODE = 0;

    private HttpURLConnection urlConnection = null;

    // Track data consumption for this task to avoid exceeding user's limit
    private long dataConsumed;

    public HttpTask(MeasurementDesc desc) {
        super(new HttpDesc(desc.key, desc.startTime, desc.endTime, desc.intervalSec,
                desc.count, desc.priority, desc.parameters, desc.instanceNumber, desc.addedToQueueAt, desc.dispatchTime));
        dataConsumed = 0;
    }

    /**
     * The description of a HTTP measurement
     */
    public static class HttpDesc extends MeasurementDesc {
        public String url;
        private String method;
        private String headers;
        private String body;

        public HttpDesc(String key, Date startTime, Date endTime,
                        double intervalSec, long count, long priority, Map<String, String> params, int instanceNumber, Date addedToQueueAt, Date dispatchTime)
                throws InvalidParameterException {
            super(HttpTask.TYPE, key, startTime, endTime, intervalSec, count, priority, params, instanceNumber, addedToQueueAt, dispatchTime);
            initializeParams(params);
            if (this.url == null || this.url.length() == 0) {
                throw new InvalidParameterException("URL for http task is null");
            }
        }

        @Override
        protected void initializeParams(Map<String, String> params) {

            if (params == null) {
                return;
            }

            this.url = (params.get("target") == null) ? params.get("url") : params.get("target");
            if (!this.url.startsWith("http://") && !this.url.startsWith("https://")) {
                this.url = "http://" + this.url;
            }

            this.method = params.get("method");
            if (this.method == null || this.method.isEmpty()) {
                this.method = "get";
            }
            this.headers = params.get("headers");
            this.body = params.get("body");
        }

        @Override
        public String getType() {
            return HttpTask.TYPE;
        }

    }

    /**
     * Returns a copy of the HttpTask
     */
    @Override
    public MeasurementTask clone() {
        MeasurementDesc desc = this.measurementDesc;
        HttpDesc newDesc = new HttpDesc(desc.key, desc.startTime, desc.endTime,
                desc.intervalSec, desc.count, desc.priority, desc.parameters, desc.instanceNumber, desc.addedToQueueAt, desc.dispatchTime);
        return new HttpTask(newDesc);
    }

    @Override
    public void stop() {

    }

    /**
     * Runs the HTTP measurement task. Will acquire power lock to ensure wifi is not turned off
     */
    @Override
    public MeasurementResult call() throws MeasurementError {

        int statusCode = HttpTask.DEFAULT_STATUS_CODE;
        long duration = 0;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        long originalHeadersLen = 0;
        String headers = null;
        ByteBuffer body = ByteBuffer.allocate(HttpTask.MAX_BODY_SIZE_TO_UPLOAD);
        boolean success = false;
        String errorMsg = "";
        InputStream responseStream = null;
        int times;
        Map<String, Integer> visited = new HashMap<>();
        URL resourceUrl, base, next;
        String location;

        HttpDesc task = (HttpDesc) this.measurementDesc;
        String urlStr = task.url;

        try {
            while (true) {
                times = visited.compute(urlStr, (key, count) -> count == null ? 1 : count + 1);
                if (times > 3)
                    throw new IOException("Stuck in redirect loop");
                resourceUrl = new URL(urlStr);
                startTime = System.currentTimeMillis();
                urlConnection = (HttpURLConnection) resourceUrl.openConnection();
                if (task.method.compareToIgnoreCase("head") == 0) {
                    urlConnection.setRequestMethod("HEAD");
                } else if (task.method.compareToIgnoreCase("get") == 0) {
                    urlConnection.setRequestMethod("GET");
                } else if (task.method.compareToIgnoreCase("post") == 0) {
                    urlConnection.setRequestMethod("POST");
                    urlConnection.setDoOutput(true);
                    try (OutputStream os = urlConnection.getOutputStream()) {
                        byte[] input = task.body.getBytes(StandardCharsets.UTF_8);
                        os.write(input, 0, input.length);
                    }
                } else {
                    urlConnection.setRequestMethod("GET");
                }
                if (task.headers != null && task.headers.trim().length() > 0) {
                    for (String headerLine : task.headers.split("\r\n")) {
                        String tokens[] = headerLine.split(":");
                        if (tokens.length == 2) {
                            urlConnection.setRequestProperty(tokens[0], tokens[1]);
                        } else {
                            throw new MeasurementError("Incorrect header line: " + headerLine);
                        }
                    }
                }
                urlConnection.setInstanceFollowRedirects(false);
                switch (urlConnection.getResponseCode()) {
                    case HttpURLConnection.HTTP_MOVED_PERM:
                    case HttpURLConnection.HTTP_MOVED_TEMP:
                    case HttpURLConnection.HTTP_SEE_OTHER:
                    case 307:
                        location = urlConnection.getHeaderField("Location");
                        location = URLDecoder.decode(location, "UTF-8");
                        base = new URL(urlStr);
                        next = new URL(base, location);  // Deal with relative URLs
                        urlStr = next.toExternalForm();
                        continue;
                }
                break;
            }
            byte[] readBuffer = new byte[HttpTask.READ_BUFFER_SIZE];
            int readLen;
            int totalBodyLen = 0;

            statusCode = urlConnection.getResponseCode();
            success = (statusCode == 200);

            responseStream = urlConnection.getInputStream();
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            while ((readLen = responseStream.read(readBuffer)) > 0
                    && totalBodyLen <= HttpTask.MAX_HTTP_RESPONSE_SIZE) {
                totalBodyLen += readLen;
                // Fill in the body to report up to MAX_BODY_SIZE
                if (body.remaining() > 0) {
                    int putLen = body.remaining() < readLen ? body.remaining() : readLen;
                    body.put(readBuffer, 0, putLen);
                }
            }

            Map<String, List<String>> headerFields = urlConnection.getHeaderFields();
            if (headerFields != null) {
                headers = "";
                for (Map.Entry<String, List<String>> header : headerFields.entrySet()) {
                    /*
                     * TODO(Wenjie): There can be preceding and trailing white spaces in
                     * each header field. I cannot find internal methods that return the
                     * number of bytes in a header. The solution here assumes the encoding
                     * is one byte per character.
                     */
                    originalHeadersLen += header.toString().length();
                    headers += header.toString() + "\r\n";
                }
            }

            MeasurementResult result = new MeasurementResult(BeanUtil.getBean(WebSocketService.class).getDeviceId(),
                    HttpTask.TYPE, System.currentTimeMillis() * 1000,
                    success, this.measurementDesc, duration);

            result.addResult("code", statusCode);
            result.addResult("expStart", startTime);
            result.addResult("expEnd", endTime);
            dataConsumed = 0;

            if (success) {
                result.addResult("time_ms", duration);
                result.addResult("headers_len", originalHeadersLen);
                result.addResult("body_len", totalBodyLen);
                result.addResult("headers", headers);
                result.addResult("body", Base64.encodeToString(body.array(), Base64.DEFAULT));
            }
            String resultJsonString = MeasurementJsonConvertor.toJsonString(result);
            logger.info(resultJsonString);
            wsSession = BeanUtil.getBean(StompSession.class);
            wsSession.send(Constants.STOMP_SERVER_JOB_RESULT_ENDPOINT, resultJsonString);
            logger.debug("HttpTask Results Sending initiated");
            return result;
        } catch (IOException e) {
            errorMsg += e.getMessage() + "\n";
            logger.error(e.getMessage());
        } catch (Exception e) {
            Log.e("HttpTask", "call: " + e.getMessage());
        } finally {
            if (responseStream != null) {
                try {
                    responseStream.close();
                } catch (IOException e) {
                    logger.error("Fails to close the input stream from the HTTP response");
                }
            }

        }
        duration = System.currentTimeMillis() - startTime;
        MeasurementResult result = new MeasurementResult(BeanUtil.getBean(WebSocketService.class).getDeviceId(), HttpTask.TYPE, System.currentTimeMillis() * 1000,
                success, this.measurementDesc, duration);
        result.addResult("code", Integer.MAX_VALUE);
        return result;
    }

    public static Class getDescClass() throws InvalidClassException {
        return HttpDesc.class;
    }

    @Override
    public String getType() {
        return HttpTask.TYPE;
    }

    @Override
    public String getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public String toString() {
        HttpDesc desc = (HttpDesc) measurementDesc;
        return "[HTTP " + desc.method + "]\n  Target: " + desc.url + "\n  Interval (sec): " +
                desc.intervalSec + "\n  Next run: " + desc.startTime;
    }

    /**
     * Data used so far by the task.
     * <p>
     * To calculate this, we measure <i>all</i> data sent while the task
     * is running. This will tend to overestimate the data consumed, but due
     * to retransmissions, etc, especially when signal strength is poor, attempting
     * to calculate the size directly will tend to greatly underestimate the data
     * consumed.
     */
    @Override
    public long getDataConsumed() {
        return dataConsumed;
    }
}
