'use strict';

const { Firestore, FieldValue } = require('@google-cloud/firestore');
const firestore = new Firestore();

const PUBLICATION_FIELDS = ['data', 'source', 'metadataFetchedAt', 'citationsFetchedAt', 'refreshAfter', 'expireAt', 'refreshQueuedAt'];

const normalizeDoi = doi => String(doi ?? "").trim().toLowerCase();
const toDoiDocId = doi => normalizeDoi(doi).replace(/\//g, '\\');

const publicationRef = doi => firestore.collection('pure-publications').doc(toDoiDocId(doi));
const prefetchCandidateRef = doi => firestore.collection('prefetch-candidates').doc(toDoiDocId(doi));
const crossrefStateRef = firestore.collection("service-state").doc("crossref");

// Hands out a document as plain object with dates instead of timestamps, or null if it does not exist
const toEntry = doc => doc.exists
    ? Object.fromEntries(Object.entries(doc.data()).map(([key, value]) => [key, value?.toDate?.() ?? value]))
    : null;

const readAll = async refs => refs.length > 0 ? (await firestore.getAll(...refs)).map(toEntry) : [];

const readPublicationEntry = async doi => toEntry(await publicationRef(doi).get());

const readPublicationEntries = dois => readAll(dois.map(publicationRef));

// Replaces the listed fields of the record (absent ones are removed) and leaves the
// prefetch marker alone, which a signal task may have written since the record was read.
const writePublicationEntry = (doi, entry) => publicationRef(doi).set(
    Object.fromEntries(PUBLICATION_FIELDS.map(field => [field, entry[field] ?? FieldValue.delete()])),
    { mergeFields: PUBLICATION_FIELDS }
);

const markRefreshQueued = doi => publicationRef(doi)
    .set({ refreshQueuedAt: FieldValue.serverTimestamp() }, { merge: true });

const readCrossrefBackoffUntil = async () => (await crossrefStateRef.get()).data()?.backoffUntil?.toDate?.();

const writeCrossrefBackoff = state => crossrefStateRef
    .set({ ...state, updatedAt: FieldValue.serverTimestamp() }, { merge: true });

const readPrefetchCandidates = dois => readAll(dois.map(prefetchCandidateRef));

// Counts the source once for each signal's target and marks the source as recorded, unless it
// already is; resolves to whether the signals were recorded.
// A signal is { doi, relationTypes, status }; without status, the candidate keeps its status.
const writePrefetchSignals = (sourceDoi, signals) => firestore.runTransaction(async transaction => {
    if ((await transaction.get(publicationRef(sourceDoi))).data()?.prefetchSignalsAt) {
        return false;
    }
    signals.forEach(signal => {
        const update = {
            doi: signal.doi,
            uniqueSourceCount: FieldValue.increment(1),
            lastSeenAt: FieldValue.serverTimestamp(),
            lastSourceDoi: sourceDoi,
            relationTypes: FieldValue.arrayUnion(...signal.relationTypes)
        };
        if (signal.status) {
            update.status = signal.status;
        }
        if (signal.status === "queued") {
            update.queuedAt = FieldValue.serverTimestamp();
        }
        transaction.set(prefetchCandidateRef(signal.doi), update, { merge: true });
    });
    transaction.set(publicationRef(sourceDoi), { prefetchSignalsAt: FieldValue.serverTimestamp() }, { merge: true });
    return true;
});

const markPrefetchCandidate = (doi, update) => prefetchCandidateRef(doi).set(update, { merge: true });

// Callers go through this object, so tests can replace single functions.
module.exports = {
    normalizeDoi,
    toDoiDocId,
    publicationRef,
    prefetchCandidateRef,
    readPublicationEntry,
    readPublicationEntries,
    writePublicationEntry,
    markRefreshQueued,
    readCrossrefBackoffUntil,
    writeCrossrefBackoff,
    readPrefetchCandidates,
    writePrefetchSignals,
    markPrefetchCandidate
};
