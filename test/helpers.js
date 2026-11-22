'use strict';

const store = require('../store');

const DAY_MS = 24 * 60 * 60 * 1000;

// Replaces the Firestore access of the store with an in-memory map of publication entries.
const stubStore = (entries = new Map()) => {
    const state = { entries, candidates: new Map(), crossrefBackoff: null };
    store.readPublicationEntry = async doi => entries.get(doi) ?? null;
    store.readPublicationEntries = async dois => dois.map(doi => entries.get(doi) ?? null);
    store.readPrefetchCandidates = async dois => dois.map(doi => state.candidates.get(doi) ?? null);
    store.writePrefetchSignals = async (sourceDoi, signals) => {
        signals.forEach(signal => {
            const candidate = state.candidates.get(signal.doi) ?? { uniqueSourceCount: 0 };
            state.candidates.set(signal.doi, {
                ...candidate,
                uniqueSourceCount: candidate.uniqueSourceCount + 1,
                status: signal.status ?? candidate.status,
                ...(signal.status === "queued" && { queuedAt: new Date() })
            });
        });
        entries.set(sourceDoi, { ...entries.get(sourceDoi), prefetchSignalsAt: new Date() });
    };
    store.markPrefetchCandidate = async (doi, update) => {
        state.candidates.set(doi, { ...state.candidates.get(doi), ...update });
    };
    store.writePublicationEntry = async (doi, entry) => {
        entries.set(doi, entry);
    };
    store.markRefreshQueued = async doi => {
        entries.set(doi, { ...entries.get(doi), refreshQueuedAt: new Date() });
    };
    store.readCrossrefBackoffUntil = async () => state.crossrefBackoff?.backoffUntil;
    store.writeCrossrefBackoff = async backoff => {
        state.crossrefBackoff = backoff;
    };
    return state;
};

// Answers provider requests by the first route whose key is part of the URL; a route is
// either a JSON body (status 200) or a status code. Unmatched requests get a 404.
// Returns the requested URLs; the URLs of enqueued Cloud Tasks are collected in its `tasks`.
const stubProviders = routes => {
    const requests = [];
    requests.tasks = [];
    global.fetch = async (url, options) => {
        requests.push(url);
        if (url.includes(CLOUD_TASKS.queue) && options?.body) {
            requests.tasks.push(JSON.parse(options.body).task.httpRequest.url);
        }
        const route = Object.entries(routes).find(([pattern]) => url.includes(pattern))?.[1];
        const status = route === undefined ? 404 : (typeof route === "number" ? route : 200);
        return {
            status: status,
            ok: status === 200,
            headers: { get: () => null },
            json: async () => route,
            text: async () => JSON.stringify(route)
        };
    };
    return requests;
};

// Routes that let Cloud Tasks enqueueing succeed
const CLOUD_TASKS = { token: "metadata.google.internal", queue: "cloudtasks.googleapis.com" };
const cloudTasksRoutes = { [CLOUD_TASKS.token]: { access_token: "token" }, [CLOUD_TASKS.queue]: { name: "task" } };

const PROVIDER = {
    crossref: "api.crossref.org",
    openCitationsMeta: "opencitations.net/meta",
    citations: "opencitations.net/index",
    openAlex: "api.openalex.org",
    dataCite: "api.datacite.org"
};

const daysFromNow = date => (date.getTime() - Date.now()) / DAY_MS;

module.exports = { stubStore, stubProviders, cloudTasksRoutes, PROVIDER, daysFromNow };
