'use strict';

const assert = require('node:assert/strict');
const { afterEach, beforeEach, test } = require('node:test');

const { refreshPublicationData } = require('../index.js').__test;
const { stubStore, stubProviders, PROVIDER } = require('./helpers');

const DOI = '10.1234/example';
const originalFetch = global.fetch;
const asks = (requests, provider) => requests.some(url => url.includes(provider));
const crossrefWork = work => ({ message: { title: ['Crossref title'], 'is-referenced-by-count': 1, ...work } });
const openAlexWork = { title: 'OpenAlex title', abstract_inverted_index: { An: [0], abstract: [1] } };
const openCitationsReferences = [
    { citing: `omid:br/1 doi:${DOI}`, cited: 'omid:br/2 doi:10.ref/one pmid:123' },
    { citing: `omid:br/1 doi:${DOI}`, cited: 'omid:br/3' },
    { citing: `omid:br/1 doi:${DOI}`, cited: 'doi:10.ref/two' }
];

const refresh = async (routes, storedEntry = null) => {
    const { entries } = stubStore(new Map(storedEntry ? [[DOI, storedEntry]] : []));
    const requests = stubProviders(routes);
    const logEntry = {};
    const data = await refreshPublicationData({ doi: DOI, cachedEntry: storedEntry, logEntry });
    return { data, requests, logEntry, entry: entries.get(DOI) };
};

beforeEach(() => {
    process.env.OPENALEX_API_KEY = 'key';
});

afterEach(() => {
    delete process.env.OPENALEX_API_KEY;
    global.fetch = originalFetch;
});

test('a Crossref record without abstract gets it from OpenAlex', async () => {
    const { data, entry } = await refresh({
        [PROVIDER.crossref]: crossrefWork({ reference: [{ DOI: '10.ref/one' }] }),
        [PROVIDER.openAlex]: openAlexWork
    });

    assert.equal(data.abstract, 'An abstract');
    assert.equal(data.title, 'Crossref title');
    assert.equal(entry.source, 'Crossref');
});

test('OpenAlex is not asked when the abstract is known', async () => {
    const fromCrossref = await refresh({ [PROVIDER.crossref]: crossrefWork({ abstract: 'Crossref abstract' }) });
    const fromStore = await refresh(
        { [PROVIDER.crossref]: crossrefWork({}) },
        { source: 'Crossref', data: { doi: DOI, title: 'Stored title', abstract: 'Stored abstract' } }
    );

    assert.equal(asks(fromCrossref.requests, PROVIDER.openAlex), false);
    assert.equal(asks(fromStore.requests, PROVIDER.openAlex), false);
    assert.equal(fromStore.data.abstract, 'Stored abstract');
});

test('a missing abstract is not looked up without an OpenAlex key', async () => {
    delete process.env.OPENALEX_API_KEY;

    const { data, requests } = await refresh({ [PROVIDER.crossref]: crossrefWork({}), [PROVIDER.openAlex]: openAlexWork });

    assert.equal(asks(requests, PROVIDER.openAlex), false);
    assert.equal(data.abstract, undefined);
});

test('a Crossref record without references gets them from OpenCitations', async () => {
    const { data, logEntry } = await refresh({
        [PROVIDER.crossref]: crossrefWork({ reference: [{ unstructured: 'A reference without DOI' }] }),
        [PROVIDER.references]: openCitationsReferences,
        [PROVIDER.citations]: [{ citing: 'doi:10.cit/one' }]
    });

    assert.equal(data.reference, '10.ref/one; 10.ref/two');
    assert.equal(data.citation, '10.cit/one; ');
    assert.equal(logEntry.openCitationsReferences.status, 200);
});

test('a record from a fallback source gets its references from OpenCitations', async () => {
    const { data, entry } = await refresh({
        [PROVIDER.dataCite]: { data: { attributes: { titles: [{ title: 'DataCite title' }] } } },
        [PROVIDER.references]: openCitationsReferences
    });

    assert.equal(entry.source, 'DataCite');
    assert.equal(data.reference, '10.ref/one; 10.ref/two');
});

test('references are not looked up when they are known or the DOI is unknown', async () => {
    const fromCrossref = await refresh({ [PROVIDER.crossref]: crossrefWork({ reference: [{ DOI: '10.ref/crossref' }] }) });
    const fromStore = await refresh(
        { [PROVIDER.crossref]: crossrefWork({}) },
        { source: 'Crossref', data: { doi: DOI, title: 'Stored title', reference: '10.ref/stored' } }
    );
    const unknown = await refresh({});

    assert.equal(asks(fromCrossref.requests, PROVIDER.references), false);
    assert.equal(asks(fromStore.requests, PROVIDER.references), false);
    assert.equal(fromStore.data.reference, '10.ref/stored');
    assert.equal(asks(unknown.requests, PROVIDER.references), false);
});
