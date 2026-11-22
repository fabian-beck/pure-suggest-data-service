'use strict';

const { Firestore, FieldValue } = require('@google-cloud/firestore');
const firestore = new Firestore();

const normalizeDoi = doi => String(doi ?? "").trim().toLowerCase();
const toDoiDocId = doi => normalizeDoi(doi).replace(/\//g, '\\');

const publicationRef = doi => firestore.collection('pure-publications').doc(toDoiDocId(doi));
const prefetchCandidateRef = doi => firestore.collection('prefetch-candidates').doc(toDoiDocId(doi));
const crossrefStateRef = firestore.collection("service-state").doc("crossref");

const readPublicationEntry = async doi => {
    const doc = await publicationRef(doi).get();
    if (!doc.exists) {
        return null;
    }
    // Timestamps are handed out as dates
    return Object.fromEntries(Object.entries(doc.data()).map(([key, value]) => [key, value?.toDate?.() ?? value]));
};

const writePublicationEntry = (doi, entry) => publicationRef(doi).set(entry);

const markRefreshQueued = doi => publicationRef(doi)
    .set({ refreshQueuedAt: FieldValue.serverTimestamp() }, { merge: true });

const readCrossrefBackoffUntil = async () => (await crossrefStateRef.get()).data()?.backoffUntil?.toDate?.();

const writeCrossrefBackoff = state => crossrefStateRef
    .set({ ...state, updatedAt: FieldValue.serverTimestamp() }, { merge: true });

// Callers go through this object, so tests can replace single functions.
module.exports = {
    firestore,
    FieldValue,
    normalizeDoi,
    toDoiDocId,
    publicationRef,
    prefetchCandidateRef,
    readPublicationEntry,
    writePublicationEntry,
    markRefreshQueued,
    readCrossrefBackoffUntil,
    writeCrossrefBackoff
};
