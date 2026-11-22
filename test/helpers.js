'use strict';

const store = require('../store');

const DAY_MS = 24 * 60 * 60 * 1000;

// Replaces the Firestore access of the store with an in-memory map of publication entries.
const stubStore = (entries = new Map()) => {
    const state = { entries, crossrefBackoff: null };
    store.readPublicationEntry = async doi => entries.get(doi) ?? null;
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
const stubProviders = routes => {
    const requests = [];
    global.fetch = async url => {
        requests.push(url);
        const route = Object.entries(routes).find(([pattern]) => url.includes(pattern))?.[1];
        const status = route === undefined ? 404 : (typeof route === "number" ? route : 200);
        return {
            status: status,
            headers: { get: () => null },
            json: async () => route
        };
    };
    return requests;
};

const PROVIDER = {
    crossref: "api.crossref.org",
    openCitationsMeta: "opencitations.net/meta",
    citations: "opencitations.net/index",
    openAlex: "api.openalex.org",
    dataCite: "api.datacite.org"
};

const daysFromNow = date => (date.getTime() - Date.now()) / DAY_MS;

module.exports = { stubStore, stubProviders, PROVIDER, daysFromNow };
