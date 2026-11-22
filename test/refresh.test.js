'use strict';

const assert = require('node:assert/strict');
const { afterEach, test } = require('node:test');

const { loadPublicationData, refreshPublicationData } = require('../index.js').__test;
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
    assert.ok(daysFromNow(entry.refreshAfter) < 1, 'retries Crossref soon');
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
    assert.ok(daysFromNow(entry.refreshAfter) <= 1);
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
    assert.ok(daysFromNow(entry.refreshAfter) > 89);
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
    assert.ok(daysFromNow(entry.refreshAfter) < 1);
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

const storedEntryOfYear = (year, fields) => {
    const entry = storedCrossrefEntry();
    delete entry.expireAt;
    entry.data.year = year;
    return { ...entry, ...fields };
};
const daysAgo = days => new Date(Date.now() - days * 24 * 60 * 60 * 1000);
const asksCrossref = requests => requests.some(url => url.includes(PROVIDER.crossref));

test('an old publication with current metadata only refreshes its citations', async () => {
    const stored = storedEntryOfYear(2010, { metadataFetchedAt: daysAgo(100), refreshAfter: daysAgo(1) });
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({ [PROVIDER.citations]: citations('10.cit/one', '10.cit/two') });

    const { data, logEntry, entry } = await refresh(entries);

    assert.equal(asksCrossref(requests), false);
    assert.equal(logEntry.metadataSource, 'stored');
    assert.equal(data.title, 'Stored title');
    assert.equal(data.citation, '10.cit/one; 10.cit/two; ');
    assert.equal(entry.source, 'Crossref');
    assert.deepEqual(entry.metadataFetchedAt, stored.metadataFetchedAt);
    assert.ok(daysFromNow(entry.citationsFetchedAt) > -1);
    assert.ok(daysFromNow(entry.refreshAfter) > 89);
    assert.equal(entry.expireAt, undefined, 'records with metadata do not expire');
});

test('metadata is refreshed once it is older than the interval for the publication age', async () => {
    const stored = storedEntryOfYear(2010, { metadataFetchedAt: daysAgo(400) });
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({
        [PROVIDER.crossref]: crossrefWork({ title: ['Fresh title'] }),
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, entry } = await refresh(entries);

    assert.equal(asksCrossref(requests), true);
    assert.equal(data.title, 'Fresh title');
    assert.ok(daysFromNow(entry.metadataFetchedAt) > -1);
});

test('a recent publication is refreshed weekly', async () => {
    const stored = storedEntryOfYear(new Date().getFullYear(), { metadataFetchedAt: daysAgo(8) });
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({ [PROVIDER.citations]: citations('10.cit/one') });

    const { entry } = await refresh(entries);

    assert.equal(asksCrossref(requests), false);
    assert.ok(daysFromNow(entry.refreshAfter) > 6 && daysFromNow(entry.refreshAfter) <= 7);
});

test('a stored fallback record asks Crossref again on every refresh', async () => {
    const stored = storedEntryOfYear(2010, { source: 'OpenAlex', metadataFetchedAt: daysAgo(1) });
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({ [PROVIDER.citations]: citations('10.cit/one') });

    await refresh(entries);

    assert.equal(asksCrossref(requests), true);
});

test('noCache refreshes metadata that is not due yet', async () => {
    const stored = storedEntryOfYear(2010, { metadataFetchedAt: daysAgo(1), refreshAfter: daysAgo(-30) });
    const { entries } = stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({
        [PROVIDER.crossref]: crossrefWork({ title: ['Fresh title'] }),
        [PROVIDER.citations]: citations('10.cit/one')
    });

    const { data, logEntry } = await loadPublicationData({ doi: DOI, noCache: true });

    assert.equal(logEntry.tag, 'cache-disabled');
    assert.equal(asksCrossref(requests), true);
    assert.equal(data.title, 'Fresh title');
});

test('an entry without metadata expires after a day instead of being refreshed', async () => {
    const { entries } = stubStore();
    stubProviders({});

    const { entry } = await refresh(entries);

    assert.equal(entry.refreshAfter, undefined);
    assert.ok(daysFromNow(entry.expireAt) > 0.9 && daysFromNow(entry.expireAt) <= 1);
});

test('a record is served without provider requests until its refresh is due', async () => {
    const stored = storedEntryOfYear(2010, { refreshAfter: daysAgo(-1) });
    stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({});

    const { data, logEntry } = await loadPublicationData({ doi: DOI });

    assert.equal(logEntry.tag, 'cache-hit');
    assert.deepEqual(data, stored.data);
    assert.equal(requests.length, 0);
});

test('a record from before the refresh schedule is served until its expiry date', async () => {
    const legacy = { ...storedCrossrefEntry(), expireAt: daysAgo(-1) };
    stubStore(new Map([[DOI, legacy]]));
    const requests = stubProviders({});

    const { logEntry } = await loadPublicationData({ doi: DOI });

    assert.equal(logEntry.tag, 'cache-hit');
    assert.equal(requests.length, 0);
});

test('a record due for refresh is served as stored and queued for refresh', async () => {
    const stored = storedEntryOfYear(2010, { refreshAfter: daysAgo(1) });
    stubStore(new Map([[DOI, stored]]));
    const requests = stubProviders({});

    const { data, logEntry } = await loadPublicationData({ doi: DOI });

    assert.equal(logEntry.tag, 'cache-stale');
    assert.deepEqual(data, stored.data);
    assert.equal(asksCrossref(requests), false);
    assert.ok(logEntry.refresh, 'tries to queue a refresh');
});
