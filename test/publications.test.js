'use strict';

const assert = require('node:assert/strict');
const { afterEach, test } = require('node:test');

const {
    addOpenCitations,
    fetchJson,
    getDoiRequestInput,
    getPrefetchTargetsByRelation,
    hasMetadata,
    isTransientCrossrefStatus,
    mergeMetadata,
    normalizeDoi,
    parseDoiInput,
    toDoiDocId
} = require('../index.js').__test;

const originalFetch = global.fetch;

afterEach(() => {
    global.fetch = originalFetch;
});

test('normalizes DOI input and Firestore document ids', () => {
    assert.equal(normalizeDoi(' 10.1111/CGF.12936 '), '10.1111/cgf.12936');
    assert.equal(toDoiDocId('10.1111/CGF.12936'), '10.1111\\cgf.12936');
    assert.deepEqual(parseDoiInput(' 10.A/One '), ['10.a/one']);
    assert.deepEqual(parseDoiInput('10.A/One; 10.B/Two,10.C/Three', true), [
        '10.a/one',
        '10.b/two',
        '10.c/three'
    ]);
});

test('extracts DOI request input from query and post body shapes', () => {
    const request = {
        method: 'POST',
        query: {
            doi: ['10.A/One', '10.B/Two'],
            dois: '10.C/Three;10.D/Four'
        },
        body: {
            doi: '10.E/Five',
            dois: ['10.F/Six,10.G/Seven']
        }
    };

    assert.deepEqual(getDoiRequestInput(request), {
        dois: [
            '10.a/one',
            '10.b/two',
            '10.c/three',
            '10.d/four',
            '10.e/five',
            '10.f/six',
            '10.g/seven'
        ],
        isBulk: true
    });
});

test('detects metadata and transient Crossref statuses', () => {
    assert.equal(hasMetadata({ title: 'A title' }), true);
    assert.equal(hasMetadata({ doi: '10.1/example' }), false);
    assert.equal(isTransientCrossrefStatus(429), true);
    assert.equal(isTransientCrossrefStatus(503), true);
    assert.equal(isTransientCrossrefStatus('timeout'), true);
    assert.equal(isTransientCrossrefStatus(404), false);
});

test('collects prefetch targets across references and citations', () => {
    const targets = getPrefetchTargetsByRelation({
        reference: '10.A/One; 10.B/Two',
        citation: '10.B/Two; 10.C/Three'
    });
    const byDoi = Object.fromEntries(targets.map(target => [
        target.doi,
        [...target.relationTypes].sort()
    ]));

    assert.deepEqual(byDoi, {
        '10.a/one': ['reference'],
        '10.b/two': ['citation', 'reference'],
        '10.c/three': ['citation']
    });
});

test('merges metadata from Crossref and normalizes all-caps author names', () => {
    const metadata = mergeMetadata('10.1234/example', {
        title: ['Crossref title'],
        published: { 'date-parts': [[2020]] },
        author: [{ family: 'DOE', given: 'JANE', ORCID: 'https://orcid.org/0000-0000-0000-0000' }],
        'container-title': ['Journal of Examples'],
        reference: [{ DOI: '10.ref/one' }, {}, { DOI: '10.ref/two' }]
    }, null, null, null);

    assert.equal(metadata.title, 'Crossref title');
    assert.equal(metadata.year, 2020);
    assert.equal(metadata.author, 'Doe, Jane, 0000-0000-0000-0000');
    assert.equal(metadata.container, 'Journal of Examples');
    assert.equal(metadata.reference, '10.ref/one; 10.ref/two');
});

test('fetchJson returns parsed JSON and logs status for successful responses', async () => {
    global.fetch = async (url, options) => {
        assert.equal(url, 'https://example.test/ok');
        assert.deepEqual(options.headers, { authorization: 'token' });
        assert.equal(options.signal.aborted, false);
        return {
            status: 200,
            json: async () => ({ ok: true })
        };
    };
    const logEntry = {};

    const result = await fetchJson({
        url: 'https://example.test/ok',
        headers: { authorization: 'token' },
        logEntry,
        logKey: 'provider'
    });

    assert.equal(result.status, 200);
    assert.deepEqual(result.data, { ok: true });
    assert.equal(logEntry.provider.status, 200);
    assert.equal(typeof logEntry.provider.processingTime, 'number');
});

test('fetchJson does not parse JSON bodies for non-200 responses', async () => {
    global.fetch = async () => ({
        status: 500,
        json: async () => {
            throw new Error('should not parse');
        }
    });
    const logEntry = {};

    const result = await fetchJson({ url: 'https://example.test/fail', logEntry, logKey: 'provider' });

    assert.equal(result.status, 500);
    assert.equal(result.data, null);
    assert.equal(logEntry.provider.status, 500);
});

test('fetchJson converts body-read failures into structured provider errors', async () => {
    global.fetch = async () => ({
        status: 200,
        json: async () => {
            throw new TypeError('terminated');
        }
    });
    const logEntry = {};

    const result = await fetchJson({ url: 'https://example.test/terminated', logEntry, logKey: 'provider' });

    assert.equal(result.status, 'error');
    assert.equal(result.data, null);
    assert.equal(logEntry.provider.status, 'error');
    assert.match(logEntry.provider.error, /terminated/);
});

test('fetchJson converts aborted requests into timeout logs', async () => {
    global.fetch = async (url, options) => new Promise((resolve, reject) => {
        options.signal.addEventListener('abort', () => {
            const error = new Error('aborted');
            error.name = 'AbortError';
            reject(error);
        });
    });
    const logEntry = {};

    const result = await fetchJson({
        url: 'https://example.test/timeout',
        logEntry,
        logKey: 'provider',
        timeoutMs: 1
    });

    assert.equal(result.status, 'timeout');
    assert.equal(logEntry.provider.status, 'timeout');
    assert.equal(logEntry.provider.timeoutMs, 1);
});

test('OpenCitations enrichment fails soft when fetch terminates', async () => {
    global.fetch = async () => {
        throw new TypeError('terminated');
    };
    const data = { doi: '10.1234/example' };
    const logEntry = {};

    await addOpenCitations('10.1234/example', data, { 'is-referenced-by-count': 0 }, logEntry);

    assert.equal(data.citation, '');
    assert.equal(logEntry.openCitations.statusCitations, 'error');
    assert.match(logEntry.openCitations.error, /terminated/);
});
