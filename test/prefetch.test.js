'use strict';

const assert = require('node:assert/strict');
const { afterEach, beforeEach, mock, test } = require('node:test');

const { loadPublicationData, processPrefetchSignalTask } = require('../index.js').__test;
const { stubStore, stubProviders, cloudTasksRoutes, PROVIDER } = require('./helpers');

const originalFetch = global.fetch;

beforeEach(() => {
    mock.method(console, 'log', () => {});
});

afterEach(() => {
    mock.restoreAll();
    global.fetch = originalFetch;
});

const sourceEntry = (doi, reference, citation) => ({
    source: 'Crossref',
    refreshAfter: new Date(Date.now() + 1000000),
    data: { doi, title: `Title of ${doi}`, reference, citation }
});

const recordSignals = async dois => {
    const prefetchContext = { signalCount: 0, enqueueAttemptCount: 0 };
    await processPrefetchSignalTask({ dois, prefetchContext });
    return prefetchContext;
};

const prefetchedDois = requests => requests.tasks.map(url => {
    const params = new URL(url).searchParams;
    assert.equal(params.get('prefetch'), 'true');
    return params.get('doi');
});

test('a target is prefetched once a second source refers to it', async () => {
    const state = stubStore(new Map([
        ['10.s/one', sourceEntry('10.s/one', '10.t/shared; 10.t/a', '10.t/b; ')],
        ['10.s/two', sourceEntry('10.s/two', '10.t/shared')]
    ]));
    const requests = stubProviders(cloudTasksRoutes);

    await recordSignals(['10.s/one']);

    assert.deepEqual([...state.candidates.keys()].sort(), ['10.t/a', '10.t/b', '10.t/shared']);
    assert.equal(state.candidates.get('10.t/shared').uniqueSourceCount, 1);
    assert.equal(state.candidates.get('10.t/shared').status, 'candidate');
    assert.ok(state.entries.get('10.s/one').prefetchSignalsAt);
    assert.deepEqual(prefetchedDois(requests), []);

    await recordSignals(['10.s/two']);

    assert.equal(state.candidates.get('10.t/shared').uniqueSourceCount, 2);
    assert.equal(state.candidates.get('10.t/shared').status, 'queued');
    assert.deepEqual(prefetchedDois(requests), ['10.t/shared']);
});

test('a source is only counted once', async () => {
    const state = stubStore(new Map([['10.s/one', sourceEntry('10.s/one', '10.t/shared')]]));
    stubProviders(cloudTasksRoutes);

    await recordSignals(['10.s/one']);
    const context = await recordSignals(['10.s/one']);

    assert.equal(state.candidates.get('10.t/shared').uniqueSourceCount, 1);
    assert.equal(context.signalCount, 0);
});

test('a source listed twice in one task is only counted once', async () => {
    const state = stubStore(new Map([['10.s/one', sourceEntry('10.s/one', '10.t/shared')]]));
    stubProviders(cloudTasksRoutes);

    await recordSignals(['10.s/one', '10.s/one']);

    assert.equal(state.candidates.get('10.t/shared').uniqueSourceCount, 1);
});

test('a source that does not fit into the signal budget is left for a later request', async () => {
    const targets = count => Array.from({ length: count }, (_, index) => `10.t/${count}-${index}`).join('; ');
    const state = stubStore(new Map([
        ['10.s/one', sourceEntry('10.s/one', targets(50), targets(49))],
        ['10.s/two', sourceEntry('10.s/two', targets(48), targets(47))],
        ['10.s/three', sourceEntry('10.s/three', targets(46), targets(45))]
    ]));
    stubProviders(cloudTasksRoutes);

    const context = await recordSignals(['10.s/one', '10.s/two', '10.s/three']);

    assert.equal(context.signalCount, 99 + 95);
    assert.ok(state.entries.get('10.s/two').prefetchSignalsAt);
    assert.equal(state.entries.get('10.s/three').prefetchSignalsAt, undefined);
    assert.equal(state.candidates.has('10.t/46-0'), false);
});

test('a target that is already stored is not prefetched', async () => {
    const state = stubStore(new Map([
        ['10.s/two', sourceEntry('10.s/two', '10.t/stored')],
        ['10.t/stored', sourceEntry('10.t/stored')]
    ]));
    state.candidates.set('10.t/stored', { uniqueSourceCount: 1, status: 'candidate' });
    const requests = stubProviders(cloudTasksRoutes);

    await recordSignals(['10.s/two']);

    assert.equal(state.candidates.get('10.t/stored').status, 'cached');
    assert.deepEqual(prefetchedDois(requests), []);
});

test('a recently queued target is not queued again', async () => {
    const state = stubStore(new Map([['10.s/two', sourceEntry('10.s/two', '10.t/queued')]]));
    state.candidates.set('10.t/queued', { uniqueSourceCount: 2, status: 'queued', queuedAt: new Date() });
    const requests = stubProviders(cloudTasksRoutes);

    await recordSignals(['10.s/two']);

    assert.equal(state.candidates.get('10.t/queued').uniqueSourceCount, 3);
    assert.deepEqual(prefetchedDois(requests), []);
});

test('the number of prefetches per request is limited', async () => {
    const targets = Array.from({ length: 8 }, (_, index) => `10.t/${index}`);
    const state = stubStore(new Map([['10.s/two', sourceEntry('10.s/two', targets.join('; '))]]));
    targets.forEach(doi => state.candidates.set(doi, { uniqueSourceCount: 1, status: 'candidate' }));
    const requests = stubProviders(cloudTasksRoutes);

    const context = await recordSignals(['10.s/two']);

    assert.equal(prefetchedDois(requests).length, 5);
    assert.equal(context.enqueueAttemptCount, 5);
    assert.equal(state.candidates.get('10.t/7').status, 'candidate');
});

test('a failed enqueue is noted on the candidate', async () => {
    const state = stubStore(new Map([['10.s/two', sourceEntry('10.s/two', '10.t/shared')]]));
    state.candidates.set('10.t/shared', { uniqueSourceCount: 1, status: 'candidate' });
    stubProviders({});

    await recordSignals(['10.s/two']);

    assert.equal(state.candidates.get('10.t/shared').status, 'enqueue-error');
    assert.ok(state.candidates.get('10.t/shared').failedAt instanceof Date);
});

test('a loaded publication reports whether its prefetch signals are still to be recorded', async () => {
    const entries = new Map([
        ['10.s/new', sourceEntry('10.s/new', '10.t/a')],
        ['10.s/recorded', { ...sourceEntry('10.s/recorded', '10.t/a'), prefetchSignalsAt: new Date() }]
    ]);
    stubStore(entries);
    stubProviders({});

    assert.equal((await loadPublicationData({ doi: '10.s/new' })).needsPrefetchSignals, true);
    assert.equal((await loadPublicationData({ doi: '10.s/recorded' })).needsPrefetchSignals, false);
    assert.equal((await loadPublicationData({ doi: '10.s/unknown' })).needsPrefetchSignals, false);
});

test('a newly loaded publication needs its prefetch signals recorded only if it has targets', async () => {
    stubStore();
    stubProviders({
        'works/10.s/with-references': { message: { title: ['A title'], reference: [{ DOI: '10.t/a' }] } },
        'works/10.s/alone': { message: { title: ['A title'] } }
    });

    assert.equal((await loadPublicationData({ doi: '10.s/with-references' })).needsPrefetchSignals, true);
    assert.equal((await loadPublicationData({ doi: '10.s/alone' })).needsPrefetchSignals, false);
});

test('a refresh keeps the mark of recorded prefetch signals', async () => {
    const prefetchSignalsAt = new Date(Date.now() - 1000);
    const { entries } = stubStore(new Map([
        ['10.s/one', { ...sourceEntry('10.s/one', '10.t/a'), prefetchSignalsAt }]
    ]));
    stubProviders({ [PROVIDER.crossref]: { message: { title: ['Fresh title'] } } });

    await loadPublicationData({ doi: '10.s/one', refreshCache: false, noCache: true });

    assert.deepEqual(entries.get('10.s/one').prefetchSignalsAt, prefetchSignalsAt);
    assert.equal(entries.get('10.s/one').data.title, 'Fresh title');
});
