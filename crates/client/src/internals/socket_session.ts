import { CLIENT_LOG_LEVEL, type ClientLogDetail } from "../public_api.js";
import { WS_CLOSE_CODE } from "../protocol_contract.js";
import type { ClientWebSocket } from "./browser_types.js";

type RuntimeLog = (level: ClientLogDetail["level"], message: string) => void;

const MAX_SERVER_FRAME_BYTES = 256 * 1024;
const BROWSER_PROTOCOL_ERROR_CLOSE_CODE = 4002;
const TEXT_ENCODER = new TextEncoder();

export class SocketSession {
    private _activeSocket: ClientWebSocket | null = null;
    private _protocolCloseCode: number | null = null;

    constructor(
        private readonly _create: (url: string) => ClientWebSocket,
        private readonly _log: RuntimeLog,
        private readonly _onOpen: () => void,
        private readonly _onMessage: (frame: string) => void,
        private readonly _onClose: (code: number) => void
    ) {}

    open(url: string): void {
        this.abort(WS_CLOSE_CODE.CLEAN);
        this._log(CLIENT_LOG_LEVEL.INFO, `opening websocket connection to ${url}`);
        const socket = this._create(url);
        socket.onopen = () => {
            this._log(CLIENT_LOG_LEVEL.INFO, "websocket opened");
            this._onOpen();
        };
        socket.onmessage = (event) => {
            if (typeof event.data !== "string") {
                this.closeForProtocolError("received non-text websocket frame");
                return;
            }
            if (TEXT_ENCODER.encode(event.data).byteLength > MAX_SERVER_FRAME_BYTES) {
                this.closeForProtocolError("received oversized websocket frame");
                return;
            }
            this._onMessage(event.data);
        };
        socket.onclose = (event) => {
            if (this._activeSocket !== socket) {
                return;
            }
            const protocolCloseCode = this._protocolCloseCode;
            const code = protocolCloseCode ?? event.code;
            this._protocolCloseCode = null;
            this._activeSocket = null;
            this._log(CLIENT_LOG_LEVEL.INFO, `websocket closed with code ${code}`);
            if (protocolCloseCode === null) {
                this._onClose(code);
            }
        };
        socket.onerror = () => undefined;
        this._activeSocket = socket;
    }

    send(frame: string): void {
        if (!this._activeSocket || this._activeSocket.readyState !== 1) {
            throw new Error("cannot send websocket frame while socket is not open");
        }
        this._activeSocket.send(frame);
    }

    close(code: number): void {
        const socket = this._activeSocket;
        if (!socket || socket.readyState >= 2 || this._protocolCloseCode !== null) {
            return;
        }
        this._protocolCloseCode = code;
        try {
            socket.close(browserCloseCode(code));
        } catch (error) {
            this._protocolCloseCode = null;
            throw error;
        }
        this._onClose(code);
    }

    abort(code: number): void {
        const socket = this._activeSocket;
        if (!socket) {
            return;
        }
        this._protocolCloseCode = null;
        socket.onclose = null;
        socket.onerror = null;
        socket.onmessage = null;
        socket.onopen = null;
        this._activeSocket = null;
        if (socket.readyState < 2) {
            socket.close(code);
        }
    }

    private closeForProtocolError(message: string): void {
        this._log(CLIENT_LOG_LEVEL.WARN, `${message} and closing with protocol error`);
        this.close(WS_CLOSE_CODE.PROTOCOL_ERROR);
    }
}

function browserCloseCode(code: number): number {
    return code === WS_CLOSE_CODE.CLEAN || (code >= 3000 && code <= 4999)
        ? code
        : BROWSER_PROTOCOL_ERROR_CLOSE_CODE;
}
