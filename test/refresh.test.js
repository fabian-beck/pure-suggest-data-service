'use strict';

const assert = require('node:assert/strict');
const { afterEach, test } = require('node:test');

const { refreshPublicationData } = require('../index.js').__test;
const { stubStore, stubProviders, PROVIDER, daysFromNow } = require('./helpers');

const DOI = '10.1234/example';
const originalFetch = global.fetch;

afterEach(() => {
    global.fetch = originalFetch;
});

const storedCrossrefEntry = () => ({
    source: 'Crossref',
    expireAt: new Date(Date.now() - 1000),
    data: {
        doi: DOI,
        title: 'Stored title',
        year: 2020,
        author: 'Doe, Jane',
        abstract: 'Stored abstract',
        reference: '10.ref/one; 10.ref/two',
        citation: '10.cit/one; '
    }
});

const crossrefWork = work => ({ message: { 'is-referenced-by-count': 2, ...work } });
const citations = (...dois) => dois.map(doi => ({ citing: `doi:${doi}` }));

const refresh = async entries => {
    const logEntry = {};
    const data = await refreshPublicationData({ doi: DOI, cachedEntry: entries.get(DOI) ?? null, logEntry });
    return { data, logEntry, entry: entries.get(DOI) };
};

test('a Crossref outage does not replace the stored record with fallback metadata', async () => {
    const { entries } = stubStore(new Map([[DOI, storedCrossrefEntry()]]));
    const requests = stubProviders({
        [PROVIDER.crossref]: 429,
        [PROVIDER.openCitationsMeta]: [{ title: 'Fallback title', author: 'Doe, J.' }],
        [PROVIDER.citations]: citations('10.cit/one', '10.cit/two')
    });

    const { data, logEntry, entry } = await refresh(entries);

    assert.equal(data.title, 'Stored title');
    assert.equal(data.reference, '10.ref/one; 10.ref/two');
    assert.equal(data.abstract, 'Stored abstract');
    assert.equal(data.citation, '10.cit/one; 10.cit/two; ');
    assert.deepEqual(entry.data, data);
    assert.equal(entry.source, 'Crossref');
    assert.equal(logEntry.cacheWrite, 'preserved-stale');
    assert.ok(daysFromNow(entry.expireAt) < 1, 'retries Crossref soon');
    assert.ok(!requests.some(url => url.includes(PROVIDER.openCitationsMeta)), 'asks no fallback provider');
});

test('a failed citation lookup keeps the stored citations and is retried within a day', async () => {
    const { entries } = stubStore(new Map([[DOI, storedCrossrefEntry()]]));
    stubProviders({
        [PROVIDER.crossref]: crossrefWork({ title: ['Fresh title'] }),
        [PROVIDER.citations]: 503
    });

    const { data, entry } = await refresh(entries);

    assert.equal(data.title, 'Fresh title');
    assert.equal(data.citation, '10.cit/one; ');
    assert.ok(daysFromNow(entry.expireAt) <= 1);
});

test('a fresh Crossref record keeps stored fields that Crossref no longer delivers', async () => {
    const { entries } = stubStore(new Map([[DOI, storedCrossrefEntry()]]));
    stubProviders({
        [PROVIDER.crossref]: crossrefWork({ title: ['Fresh title'], volume: '7' }),
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, logEntry, entry } = await refresh(entries);

    assert.equal(data.title, 'Fresh title');
    assert.equal(data.volume, '7');
    assert.equal(data.abstract, 'Stored abstract');
    assert.equal(data.reference, '10.ref/one; 10.ref/two');
    assert.deepEqual(logEntry.keptStoredFields, ['year', 'author', 'abstract', 'reference']);
    assert.ok(daysFromNow(entry.expireAt) > 29);
});

test('a fallback source only fills the gaps of a stored Crossref record', async () => {
    const { entries } = stubStore(new Map([[DOI, storedCrossrefEntry()]]));
    stubProviders({
        [PROVIDER.openCitationsMeta]: [{ title: 'Fallback title', volume: '7' }],
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, entry } = await refresh(entries);

    assert.equal(data.title, 'Stored title');
    assert.equal(data.volume, '7');
    assert.equal(entry.source, 'Crossref');
});

test('Crossref metadata replaces a stored fallback record', async () => {
    const stored = { ...storedCrossrefEntry(), source: 'OpenCitations Meta' };
    const { entries } = stubStore(new Map([[DOI, stored]]));
    stubProviders({
        [PROVIDER.crossref]: crossrefWork({ title: ['Crossref title'] }),
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, entry } = await refresh(entries);

    assert.equal(data.title, 'Crossref title');
    assert.equal(data.abstract, 'Stored abstract');
    assert.equal(entry.source, 'Crossref');
});

test('a record without any provider result keeps its stored metadata and source', async () => {
    const { entries } = stubStore(new Map([[DOI, storedCrossrefEntry()]]));
    stubProviders({});

    const { data, entry } = await refresh(entries);

    assert.equal(data.title, 'Stored title');
    assert.equal(data.citation, '10.cit/one; ');
    assert.equal(entry.source, 'Crossref');
});

test('a new publication resolved by a fallback during a Crossref outage is retried soon', async () => {
    const { entries } = stubStore();
    stubProviders({
        [PROVIDER.crossref]: 503,
        [PROVIDER.openCitationsMeta]: [{ title: 'Fallback title' }],
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, entry } = await refresh(entries);

    assert.equal(data.title, 'Fallback title');
    assert.equal(entry.source, 'OpenCitations Meta');
    assert.ok(daysFromNow(entry.expireAt) < 1);
});

test('nothing is stored when a Crossref outage leaves a new publication without metadata', async () => {
    const { entries } = stubStore();
    stubProviders({ [PROVIDER.crossref]: 503 });

    const { data, logEntry } = await refresh(entries);

    assert.deepEqual(data, { doi: DOI });
    assert.equal(entries.has(DOI), false);
    assert.equal(logEntry.cacheWrite, 'skipped-transient-crossref-failure');
});

test('citations are not loaded for a stored highly cited record while Crossref is unavailable', async () => {
    const stored = storedCrossrefEntry();
    delete stored.data.citation;
    stored.data.tooManyCitations = true;
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({ [PROVIDER.crossref]: 503 });

    const { data } = await refresh(entries);

    assert.equal(data.tooManyCitations, true);
    assert.ok(!requests.some(url => url.includes(PROVIDER.citations)));
});
