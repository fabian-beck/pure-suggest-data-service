'use strict';

const { Firestore, FieldValue } = require('@google-cloud/firestore');
const firestore = new Firestore();

const WRITE_BATCH_SIZE = 200;

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

const writePublicationEntry = (doi, entry) => publicationRef(doi).set(entry);

const markRefreshQueued = doi => publicationRef(doi)
    .set({ refreshQueuedAt: FieldValue.serverTimestamp() }, { merge: true });

const readCrossrefBackoffUntil = async () => (await crossrefStateRef.get()).data()?.backoffUntil?.toDate?.();

const writeCrossrefBackoff = state => crossrefStateRef
    .set({ ...state, updatedAt: FieldValue.serverTimestamp() }, { merge: true });

const readPrefetchCandidates = dois => readAll(dois.map(prefetchCandidateRef));

// Counts the source once for each signal's target and marks the source as recorded.
// A signal is { doi, relationTypes, status }; without status, the candidate keeps its status.
const writePrefetchSignals = async (sourceDoi, signals) => {
    for (let start = 0; start < signals.length; start += WRITE_BATCH_SIZE) {
        const batch = firestore.batch();
        signals.slice(start, start + WRITE_BATCH_SIZE).forEach(signal => {
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
            batch.set(prefetchCandidateRef(signal.doi), update, { merge: true });
        });
        if (start + WRITE_BATCH_SIZE >= signals.length) {
            batch.set(publicationRef(sourceDoi), { prefetchSignalsAt: FieldValue.serverTimestamp() }, { merge: true });
        }
        await batch.commit();
    }
};

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
