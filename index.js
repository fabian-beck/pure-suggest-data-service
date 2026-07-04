
'use strict';

const functions = require('@google-cloud/functions-framework');

const firebaseFunctions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp();
const firestore = admin.firestore();

const CROSSREF_BACKOFF_MS = 5 * 60 * 1000;
const CROSSREF_USER_AGENT = "pure-suggest-data-service/0.0.1 (mailto:fabian.beck@uni-bamberg.de)";

const isTransientCrossrefStatus = status => status === 429 || (status >= 500 && status < 600) || status === "backoff" || status === "error";
const hasMetadata = entry => Boolean(entry?.title || entry?.author || entry?.container || entry?.abstract);

const getCrossrefBackoffUntil = response => {
    const retryAfter = response?.headers?.get?.("retry-after");
    const retryAfterSeconds = Number(retryAfter);
    if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
        return new Date(Date.now() + retryAfterSeconds * 1000);
    }

    const retryAfterDate = retryAfter ? new Date(retryAfter) : null;
    if (retryAfterDate && !Number.isNaN(retryAfterDate.getTime()) && retryAfterDate > new Date()) {
        return retryAfterDate;
    }

    return new Date(Date.now() + CROSSREF_BACKOFF_MS);
};

const getCacheExpireDate = crossrefStatus => {
    const expireDate = new Date();
    expireDate.setDate(expireDate.getDate() + (crossrefStatus === 200 ? 30 : 1));
    return expireDate;
};

const getShortRetryExpireDate = () => {
    const expireDate = new Date();
    expireDate.setMinutes(expireDate.getMinutes() + 15);
    return expireDate;
};

const fetchCrossref = async (doi, logEntry) => {
    const crossrefStateRef = firestore.collection("service-state").doc("crossref");
    const crossrefStateDoc = await crossrefStateRef.get();
    const crossrefBackoffUntil = crossrefStateDoc.data()?.backoffUntil?.toDate?.();

    if (crossrefBackoffUntil > new Date()) {
        logEntry.crossref = {
            status: "backoff",
            backoffUntil: crossrefBackoffUntil.toISOString()
        };
        return { status: "backoff", data: null, transientFailure: true };
    }

    const timeStartCrossref = new Date().getTime();
    try {
        const responseCrossref = await fetch(`https://api.crossref.org/v1/works/${doi}?mailto=fabian.beck@uni-bamberg.de`, {
            headers: {
                "User-Agent": CROSSREF_USER_AGENT,
            }
        });
        const dataCrossref = responseCrossref.status === 200 ? (await responseCrossref.json())?.message : null;
        const transientFailure = isTransientCrossrefStatus(responseCrossref.status);
        logEntry.crossref = { status: responseCrossref.status, processingTime: new Date().getTime() - timeStartCrossref };

        if (transientFailure) {
            const backoffUntil = getCrossrefBackoffUntil(responseCrossref);
            logEntry.crossref.backoffUntil = backoffUntil.toISOString();
            await crossrefStateRef.set({
                backoffUntil: backoffUntil,
                status: responseCrossref.status,
                updatedAt: admin.firestore.FieldValue.serverTimestamp()
            }, { merge: true });
        }

        return { status: responseCrossref.status, data: dataCrossref, transientFailure: transientFailure };
    } catch (error) {
        const backoffUntil = getCrossrefBackoffUntil();
        logEntry.crossref = {
            status: "error",
            error: String(error),
            processingTime: new Date().getTime() - timeStartCrossref,
            backoffUntil: backoffUntil.toISOString()
        };
        await crossrefStateRef.set({
            backoffUntil: backoffUntil,
            status: "error",
            error: String(error).slice(0, 500),
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });

        return { status: "error", data: null, transientFailure: true };
    }
};

const fetchDataCite = async (doi, logEntry) => {
    const timeStartDataCite = new Date().getTime();
    const responseDataCite = await fetch(`https://api.datacite.org/dois/${doi}`);
    const dataDataCite = responseDataCite.status === 200 ? (await responseDataCite.json())?.data : null;
    logEntry.dataCite = { status: responseDataCite.status, processingTime: new Date().getTime() - timeStartDataCite };
    return dataDataCite;
};

const mergeMetadata = (doi, dataCrossref, dataDataCite) => {
    const data = { doi: doi };
    data.title = dataCrossref?.title?.[0]
        || dataDataCite?.attributes?.titles?.[0]?.title;
    data.subtitle = dataCrossref?.subtitle?.[0];
    data.year = dataCrossref?.published?.['date-parts']?.[0]?.[0]
        || dataDataCite?.attributes?.publicationYear
        || doi.match(/\.((19|20)\d\d)\./)?.[1];
    data.author = dataCrossref?.author?.reduce((acc, author) => acc + author.family
        + (author.given ? ", " + author.given : "")
        + (author.ORCID ? ", " + author.ORCID.replace(/http(s?):\/\/orcid.org\//g, "") : "")
        + "; ", "").slice(0, -2)
        || dataDataCite?.attributes?.creators?.reduce((acc, author) => acc + author.name + "; ", "").slice(0, -2);
    data.author = data.author?.replace(/(\w)(\w+)(\W?)/g, (match, p1, p2, p3) => p1 + p2.toLowerCase() + p3); // auto-correct ALLCAPS author names
    data.container = dataCrossref?.["container-title"]?.[0]
        || dataDataCite?.attributes?.relatedItems?.[0]?.titles?.[0]?.title;
    data.volume = dataCrossref?.volume;
    data.issue = dataCrossref?.issue;
    data.page = dataCrossref?.page;
    data.abstract = dataCrossref?.abstract
        || dataDataCite?.attributes?.descriptions?.[0]?.description;
    data.reference = dataCrossref?.reference?.reduce((acc, reference) =>
        (reference.DOI ? acc + reference.DOI + "; " : acc), "").slice(0, -2);
    return data;
};

const addOpenCitations = async (doi, data, dataCrossref, logEntry) => {
    // if not too many citations (is-referenced-by-count)
    if (!dataCrossref || dataCrossref["is-referenced-by-count"] < 1000) {
        const timeStartOpenCitations = new Date().getTime();
        data.citation = "";
        const responseOCCitations = await fetch(`https://opencitations.net/index/api/v2/citations/doi:${doi}`, {
            headers: {
                authorization: "aa9da96d-3c7b-49c1-a2d8-1c2d01ae10a5",
            }
        });
        const dataOCCitations = responseOCCitations.status === 200 ? (await responseOCCitations.json()) : null;
        dataOCCitations?.forEach(reference => {
            // extract doi from citing
            const doi = reference.citing.match(/doi:(\S*)\s?/i)?.[1];
            if (doi) {
                data.citation += doi + "; ";
            }
        });
        logEntry.openCitations = {
            statusCitations: responseOCCitations.status,
            processingTime: new Date().getTime() - timeStartOpenCitations
        };
    } else {
        data.tooManyCitations = true;
    }
};

const refreshPublicationData = async ({ doi, doiRef, cachedEntry, noCache, logEntry }) => {
    const cachedData = cachedEntry?.data;
    const crossref = await fetchCrossref(doi, logEntry);
    const dataCrossref = crossref.data;
    let dataDataCite = null;

    if (!dataCrossref) {
        dataDataCite = await fetchDataCite(doi, logEntry);
    }

    let data;
    try {
        data = mergeMetadata(doi, dataCrossref, dataDataCite);
    } catch (error) {
        console.error("Error processing metadata: " + error + "\n" + JSON.stringify(dataCrossref) + "\n" + JSON.stringify(dataDataCite) + "\n" + JSON.stringify(logEntry));
        return { error: "Error processing metadata: " + error };
    }

    await addOpenCitations(doi, data, dataCrossref, logEntry);

    // remove undefined/empty properties from data
    Object.keys(data).forEach(key => (data[key] === undefined || data[key] === '') && delete data[key]);

    if (!noCache && crossref.transientFailure && !hasMetadata(data) && hasMetadata(cachedData)) {
        logEntry.cacheWrite = "preserved-stale";
        data = cachedData;
        await doiRef.set({ expireAt: getShortRetryExpireDate(), data: data, source: cachedEntry.source || "stale" });
    } else if (crossref.transientFailure && !hasMetadata(data)) {
        logEntry.cacheWrite = "skipped-transient-crossref-failure";
    } else {
        logEntry.cacheWrite = "stored";
        await doiRef.set({ expireAt: getCacheExpireDate(crossref.status), data: data, source: dataDataCite ? "DataCite" : "Crossref" });
    }

    return data;
};

const logRequest = (logEntry, data, timeStart) => {
    logEntry.title = data?.title;
    logEntry.processingTime = new Date().getTime() - timeStart;
    console.log(JSON.stringify(logEntry));
};

// Retrieve aggregated data from Crossref (or DataCite) and OpenCitations
// deploy with: "gcloud functions deploy pure-publications --gen2 --runtime=nodejs20 --region=europe-west1 --source=. --entry-point=pure-publications --trigger-http --allow-unauthenticated"
functions.http('pure-publications', async (req, res) => {
    const timeStart = new Date().getTime();
    const logEntry = { severity: "INFO" };

    res.set('Content-Type', 'application/json');
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET');

    if (req.method === "OPTIONS") {
        // stop preflight requests here
        res.status(204).send('');
        return;
    }

    if (typeof req.query.doi !== "string" || !req.query.doi) {
        res.status(400).send('Missing DOI parameter');
        return;
    }

    const doi = req.query.doi.toLowerCase();
    const noCache = req.query.noCache === "true";
    logEntry.doi = doi;

    // Convert forward to backward slashes in doi
    const doi2 = doi.replace(/\//g, '\\');
    const doiRef = firestore.collection('pure-publications').doc(doi2);
    const doiDoc = await doiRef.get();
    const cachedEntry = doiDoc.exists ? doiDoc.data() : null;
    const cachedData = cachedEntry?.data;
    const cachedExpireAt = cachedEntry?.expireAt?.toDate?.();

    if (!noCache && cachedExpireAt > new Date()) {
        logEntry.tag = "cache-hit";
        logRequest(logEntry, cachedData, timeStart);
        res.send(cachedData);
        return;
    }

    if (!noCache && hasMetadata(cachedData)) {
        logEntry.tag = "cache-stale";
        if (cachedExpireAt) {
            logEntry.cacheExpireAt = cachedExpireAt.toISOString();
        }
        logEntry.refresh = "started";
        logRequest(logEntry, cachedData, timeStart);
        res.send(cachedData);

        const refreshLogEntry = {
            severity: "INFO",
            doi: doi,
            tag: "cache-refresh"
        };
        if (cachedExpireAt) {
            refreshLogEntry.cacheExpireAt = cachedExpireAt.toISOString();
        }
        const refreshStart = new Date().getTime();
        try {
            const refreshedData = await refreshPublicationData({ doi, doiRef, cachedEntry, noCache, logEntry: refreshLogEntry });
            logRequest(refreshLogEntry, refreshedData, refreshStart);
        } catch (error) {
            refreshLogEntry.severity = "ERROR";
            refreshLogEntry.error = String(error);
            logRequest(refreshLogEntry, cachedData, refreshStart);
        }
        return;
    }

    logEntry.tag = noCache ? "cache-disabled" : (cachedEntry ? "cache-expired" : "cache-miss");
    if (cachedExpireAt) {
        logEntry.cacheExpireAt = cachedExpireAt.toISOString();
    }

    const data = await refreshPublicationData({ doi, doiRef, cachedEntry, noCache, logEntry });
    logRequest(logEntry, data, timeStart);
    res.send(data);
});
