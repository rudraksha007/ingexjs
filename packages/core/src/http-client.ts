import { type BaseHttpClient, type CheerioCrawlingContext, type HttpRequest, type HttpResponse, type ResponseTypes, type StreamingHttpResponse } from "crawlee";

import { Readable } from "stream"
import type { ReadableStream } from "stream/web";


/**
 * Custom HTTP client for the Ingestor using native fetch.
 * Implements the BaseHttpClient interface from Crawlee to integrate with its crawling features.
 */
export const IngexHttpClient: BaseHttpClient = {
    /**
     * Sends a standard HTTP request and returns the parsed response.
     */
    async sendRequest<T extends keyof ResponseTypes = 'text'>(
        request: HttpRequest<T>,
    ): Promise<HttpResponse<T>> {
        const response = await fetch(request.url.toString(), {
            method: request.method,
            headers: request.headers as HeadersInit,
            body: request.body as BodyInit,
            signal: request.signal,
            redirect: request.followRedirect === false ? 'manual' : 'follow',
        });

        let body: ResponseTypes[T];

        switch (request.responseType) {
            case 'buffer':
                body = Buffer.from(await response.arrayBuffer()) as ResponseTypes[T];
                break;

            case 'json':
                body = await response.json() as ResponseTypes[T];
                break;

            default:
                body = await response.text() as ResponseTypes[T];
        }

        return {
            statusCode: response.status,
            statusMessage: response.statusText,
            headers: Object.fromEntries(response.headers.entries()),
            url: response.url,
            body,
        } as HttpResponse<T>;
    },

    /**
     * Sends an HTTP request and returns a streaming response.
     */
    async stream(
        request: HttpRequest,
    ): Promise<StreamingHttpResponse> {
        const response = await fetch(request.url.toString(), {
            method: request.method,
            headers: request.headers as HeadersInit,
            body: request.body as BodyInit,
            signal: request.signal,
            redirect: request.followRedirect === false ? 'manual' : 'follow',
        });

        return {
            statusCode: response.status,
            statusMessage: response.statusText,
            headers: Object.fromEntries(response.headers.entries()),
            url: response.url,

            stream: Readable.fromWeb(
                response.body! as any as ReadableStream<Uint8Array<ArrayBuffer>>,
            ),

            downloadProgress: {
                transferred: 0,
                total: 0,
                percent: 0,
            },

            uploadProgress: {
                transferred: 0,
                total: 0,
                percent: 0,
            },
        } as StreamingHttpResponse;
    },
};




