/** @typedef {typeof import("../../dist/odoo_sfu.js")} SfuModule */
/** @typedef {import("../../dist/odoo_sfu.js").SFU_CLIENT_STATE} SfuClientStateCatalog */

/**
 * @param {SfuModule} sfuModule
 * @param {SfuClientStateCatalog} stateCatalog
 */
function consumeSfuModule(sfuModule, stateCatalog) {
    const client = new sfuModule.SfuClient();
    const state = stateCatalog.CONNECTED;

    /** @type {import("../../dist/odoo_sfu.js").SfuClient} */
    const typedClient = client;

    // @ts-expect-error A client instance is not a constructor.
    const invalidClient = new typedClient();
    // @ts-expect-error State catalog members are fixed.
    const invalidMember = stateCatalog.UNKNOWN;

    return { client, invalidClient, invalidMember, state };
}

void consumeSfuModule;
