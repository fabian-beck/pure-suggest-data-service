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
    assert.equal(requests.length, 0);
    assert.equal(entry.source, 'Crossref');
    assert.ok(entry.metadataFetchedAt instanceof Date && entry.citationsFetchedAt instanceof Date);
    assert.ok(entry.refreshAfter > new Date());
    assert.equal(entry.expireAt, undefined);
});

test('prefetch signals add one count per source to each target and mark the source', { skip }, async () => {
    const targets = ['10.1234/Integration.Target.A', '10.1234/Integration.Target.B'].map(store.normalizeDoi);
    await Promise.all(targets.map(doi => store.prefetchCandidateRef(doi).delete()));
    await store.writePublicationEntry(DOI, { data: { doi: DOI, title: 'A title' }, source: 'Crossref' });

    assert.deepEqual(await store.readPrefetchCandidates(targets), [null, null]);

    await store.writePrefetchSignals(DOI, [
        { doi: targets[0], relationTypes: ['reference'], status: 'candidate' },
        { doi: targets[1], relationTypes: ['reference', 'citation'], status: 'candidate' }
    ]);
    await store.writePrefetchSignals('10.1234/other.source', [
        { doi: targets[0], relationTypes: ['citation'], status: 'queued' },
        { doi: targets[1], relationTypes: ['citation'] }
    ]);
    await store.markPrefetchCandidate(targets[1], { status: 'fetched', fetchedAt: new Date() });
    const [first, second] = await store.readPrefetchCandidates(targets);
    const [source] = await store.readPublicationEntries([DOI]);

    assert.equal(first.uniqueSourceCount, 2);
    assert.equal(first.status, 'queued');
    assert.ok(first.queuedAt instanceof Date);
    assert.deepEqual(first.relationTypes, ['reference', 'citation']);
    assert.equal(second.uniqueSourceCount, 2);
    assert.equal(second.status, 'fetched');
    assert.ok(second.fetchedAt instanceof Date);
    assert.ok(source.prefetchSignalsAt instanceof Date);
    assert.equal(source.data.title, 'A title');
    await store.publicationRef('10.1234/other.source').delete();
});

test('a source recorded by two tasks at the same time is counted once', { skip }, async () => {
    const target = '10.1234/integration.target.c';
    await store.prefetchCandidateRef(target).delete();
    await store.writePublicationEntry(DOI, { data: { doi: DOI, title: 'A title' }, source: 'Crossref' });
    const signals = [{ doi: target, relationTypes: ['reference'], status: 'candidate' }];

    const recorded = await Promise.all([
        store.writePrefetchSignals(DOI, signals),
        store.writePrefetchSignals(DOI, signals)
    ]);
    const [candidate] = await store.readPrefetchCandidates([target]);

    assert.deepEqual([...recorded].sort(), [false, true]);
    assert.equal(candidate.uniqueSourceCount, 1);
    assert.equal(await store.writePrefetchSignals(DOI, signals), false);
});

test('rewriting a record replaces its fields but keeps the prefetch marker', { skip }, async () => {
    await store.writePublicationEntry(DOI, {
        data: { doi: DOI, title: 'A title', tooManyCitations: true },
        source: 'OpenAlex',
        expireAt: new Date()
    });
    await store.markRefreshQueued(DOI);
    await store.writePrefetchSignals(DOI, []);

    await store.writePublicationEntry(DOI, { data: { doi: DOI, title: 'New title' }, source: 'Crossref' });
    const entry = await store.readPublicationEntry(DOI);

    assert.deepEqual(entry.data, { doi: DOI, title: 'New title' });
    assert.ok(entry.prefetchSignalsAt instanceof Date);
    assert.deepEqual(Object.keys(entry).sort(), ['data', 'prefetchSignalsAt', 'source']);
});

test('a Crossref backoff is stored and known without reading it again', { skip }, async () => {
    const backoffUntil = new Date(Date.now() + 60 * 60 * 1000);

    await store.writeCrossrefBackoff({ backoffUntil, status: 429 });
    const known = await store.readCrossrefBackoffUntil();
    await store.writeCrossrefBackoff({ backoffUntil: new Date(0), status: 200 });

    assert.deepEqual(known, backoffUntil);
    assert.deepEqual(await store.readCrossrefBackoffUntil(), new Date(0));
});
