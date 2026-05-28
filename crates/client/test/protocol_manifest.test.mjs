import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import ts from "typescript";

import {
    COMMAND_KIND,
    NEGOTIATION_KIND,
    PENDING_REQUEST_KIND,
    RECORDING_STOP_CODES,
    SOURCE_ENCODING_POLICY_ROLES,
    STREAM_TYPES,
    UPLOAD_KINDS,
    WS_CLOSE_CODE
} from "../dist/protocol_contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(
    readFileSync(path.join(ROOT, "src/generated/protocol_manifest.json"), "utf8")
);

test("runtime protocol constants match the Rust manifest", () => {
    assert.deepEqual([...STREAM_TYPES], manifest.streamTypes);
    assert.deepEqual([...UPLOAD_KINDS], manifest.uploadKinds);
    assert.deepEqual([...SOURCE_ENCODING_POLICY_ROLES], manifest.sourceEncodingPolicyRoles);
    assert.deepEqual([...RECORDING_STOP_CODES], manifest.recordingStopCodes);
    assert.deepEqual(sortObject(NEGOTIATION_KIND), manifest.negotiationKind);
    assert.deepEqual(sortObject(PENDING_REQUEST_KIND), manifest.pendingRequestKind);
    assert.deepEqual(sortObject(COMMAND_KIND), manifest.commandKind);
    assert.deepEqual(sortObject(WS_CLOSE_CODE), manifest.wsCloseCode);
});

test("TypeScript envelope aliases match the Rust manifest", () => {
    const aliases = protocolAliases();
    assert.deepEqual(
        {
            clientMessage: envelopeTags(aliases, "ClientMessageEnvelope", "message"),
            clientRequest: envelopeTags(aliases, "ClientRequestEnvelope", "request"),
            clientResponse: envelopeTags(aliases, "ClientResponseEnvelope", "response"),
            serverMessage: envelopeTags(aliases, "ServerMessageEnvelope", "message"),
            serverRequest: envelopeTags(aliases, "ServerRequestEnvelope", "request"),
            serverResponse: envelopeTags(aliases, "ServerResponseEnvelope", "response")
        },
        manifest.envelopes
    );
});

function protocolAliases() {
    const sourcePath = path.join(ROOT, "src/protocol_contract.ts");
    const configPath = path.join(ROOT, "tsconfig.json");
    const config = ts.readConfigFile(configPath, ts.sys.readFile);
    if (config.error) {
        throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"));
    }
    const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, ROOT);
    const program = ts.createProgram([sourcePath], parsed.options);
    const diagnostics = ts.getPreEmitDiagnostics(program);
    assert.deepEqual(
        diagnostics.map((diagnostic) =>
            ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")
        ),
        []
    );
    const checker = program.getTypeChecker();
    const source = program.getSourceFile(sourcePath);
    assert.ok(source, "protocol_contract.ts must be part of the TypeScript program");
    return { checker, source };
}

function envelopeTags({ checker, source }, aliasName, kind) {
    const alias = source.statements.find(
        (statement) => ts.isTypeAliasDeclaration(statement) && statement.name.text === aliasName
    );
    assert.ok(alias, `${aliasName} must exist`);
    return unionMembers(checker.getTypeFromTypeNode(alias.type)).map((member) => ({
        kind,
        tag: stringLiteralProperty(checker, member, alias, "t")
    }));
}

function unionMembers(type) {
    return type.isUnion() ? type.types : [type];
}

function stringLiteralProperty(checker, type, node, propertyName) {
    const property = checker.getPropertyOfType(type, propertyName);
    assert.ok(property, `${propertyName} property must exist`);
    const propertyType = checker.getTypeOfSymbolAtLocation(property, node);
    assert.equal(
        propertyType.isStringLiteral(),
        true,
        `${propertyName} property must be a string literal`
    );
    return propertyType.value;
}

function sortObject(value) {
    return Object.fromEntries(
        Object.entries(value).sort(([left], [right]) => left.localeCompare(right))
    );
}
