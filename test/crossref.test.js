'use strict';

const assert = require('node:assert/strict');
const { afterEach, test } = require('node:test');

const { loadBulkPublicationData } = require('../index.js').__test;
const { stubStore, PROVIDER } = require('./helpers');

const originalFetch = global.fetch;
const dois = Array.from({ length: 10 }, (_, index) => `10.c/${index}`);

afterEach(() => {
    global.fetch = originalFetch;
});

// Lets Crossref answer every request with the status after a short delay and tracks its load.
const stubSlowCrossref = status => {
    const load = { requests: 0, active: 0, maxActive: 0 };
    global.fetch = async url => {
        const response = { status: 404, headers: { get: () => null }, json: async () => ({ message: { title: ['A title'] } }) };
        if (!url.includes(PROVIDER.crossref)) {
            return response;
        }
        load.requests++;
        load.active++;
        load.maxActive = Math.max(load.maxActive, load.active);
        await new Promise(resolve => setTimeout(resolve, 5));
        load.active--;
        return { ...response, status: status };
    };
    return load;
};

const loadAll = () => loadBulkPublicationData({ dois, dependencies: { logRequest: () => {} } });

test('a bulk request keeps at most three Crossref requests in flight', async () => {
    stubStore();
    const load = stubSlowCrossref(200);

    const results = await loadAll();

    assert.equal(load.requests, 10);
    assert.equal(load.maxActive, 3);
    assert.ok(results.every(result => result.data.title === 'A title'));
});

test('requests waiting for Crossref are not sent once it asks to back off', async () => {
    stubStore();
    const load = stubSlowCrossref(429);

    const results = await loadAll();

    assert.equal(load.requests, 3);
    assert.ok(results.every(result => result.logEntry.crossref.backoffUntil));
});
