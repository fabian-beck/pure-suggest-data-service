'use strict';

// Runs against the Firestore emulator and is skipped without it:
// npx firebase-tools emulators:exec --only firestore --project demo-pure "npm test"

const assert = require('node:assert/strict');
const { afterEach, beforeEach, test } = require('node:test');

const store = require('../store');
const { loadPublicationData } = require('../index.js').__test;
const { stubProviders, PROVIDER } = require('./helpers');

const realStore = { ...store };
const skip = !process.env.FIRESTORE_EMULATOR_HOST;
const DOI = '10.1234/Integration.Example';
const originalFetch = global.fetch;

beforeEach(async () => {
    Object.assign(store, realStore);
    if (!skip) {
        await store.publicationRef(DOI).delete();
    }
});

afterEach(() => {
    global.fetch = originalFetch;
});

test('publication entries round-trip through Firestore with dates', { skip }, async () => {
    const refreshAfter = new Date('2030-01-01T00:00:00Z');

    assert.equal(await store.readPublicationEntry(DOI), null);

    await store.writePublicationEntry(DOI, { data: { doi: DOI, title: 'A title' }, source: 'Crossref', refreshAfter });
    await store.markRefreshQueued(DOI);
    const entry = await store.readPublicationEntry(DOI);

    assert.deepEqual(entry.data, { doi: DOI, title: 'A title' });
    assert.deepEqual(entry.refreshAfter, refreshAfter);
    assert.ok(entry.refreshQueuedAt instanceof Date);
});

test('a loaded publication is stored and then served without provider requests', { skip }, async () => {
    const doi = DOI.toLowerCase();
    stubProviders({
        [PROVIDER.crossref]: { message: { title: ['A title'], published: { 'date-parts': [[2015]] }, 'is-referenced-by-count': 1 } },
        [PROVIDER.citations]: [{ citing: 'doi:10.cit/one' }]
    });

    const first = await loadPublicationData({ doi });
    const requests = stubProviders({});
    const second = await loadPublicationData({ doi });
    const entry = await store.readPublicationEntry(doi);

    assert.equal(first.logEntry.tag, 'cache-miss');
    assert.equal(second.logEntry.tag, 'cache-hit');
    assert.deepEqual(second.data, { doi, title: 'A title', year: 2015, citation: '10.cit/one; ' });
    assert.deepEqual(requests, []);
    assert.equal(entry.source, 'Crossref');
    assert.ok(entry.metadataFetchedAt instanceof Date && entry.citationsFetchedAt instanceof Date);
    assert.ok(entry.refreshAfter > new Date());
    assert.equal(entry.expireAt, undefined);
});
