
'use strict';

const crypto = require('crypto');
const functions = require('@google-cloud/functions-framework');

const firebaseFunctions = require('firebase-functions');
const admin = require('firebase-admin');
admin.initializeApp();
const firestore = admin.firestore();

const CROSSREF_BACKOFF_MS = 5 * 60 * 1000;
const CROSSREF_USER_AGENT = "pure-suggest-data-service/0.0.1 (mailto:fabian.beck@uni-bamberg.de)";
const OPENCITATIONS_ACCESS_TOKEN = "aa9da96d-3c7b-49c1-a2d8-1c2d01ae10a5";
const OPENALEX_API_KEY = process.env.OPENALEX_API_KEY;
const CLOUD_REGION = "europe-west1";
const PROJECT_ID = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT || "pure-suggest-data-service";
const REFRESH_QUEUE = "pure-publications-refresh";
const PREFETCH_QUEUE = "pure-publications-prefetch";
const FUNCTION_URL = `https://${CLOUD_REGION}-${PROJECT_ID}.cloudfunctions.net/pure-publications`;
const MAX_BULK_DOIS = 50;
const PREFETCH_SIGNAL_THRESHOLD = Number(process.env.PREFETCH_SIGNAL_THRESHOLD || 2);
const PREFETCH_MAX_DOIS_PER_RELATION = Number(process.env.PREFETCH_MAX_DOIS_PER_RELATION || 50);
const PREFETCH_MAX_SIGNALS_PER_REQUEST = Number(process.env.PREFETCH_MAX_SIGNALS_PER_REQUEST || 200);
const PREFETCH_MAX_ENQUEUES_PER_REQUEST = Number(process.env.PREFETCH_MAX_ENQUEUES_PER_REQUEST || 5);
const PREFETCH_REQUEUE_COOLDOWN_MS = Number(process.env.PREFETCH_REQUEUE_COOLDOWN_MS || 24 * 60 * 60 * 1000);

const isTransientCrossrefStatus = status => status === 429 || (status >= 500 && status < 600) || status === "backoff" || status === "error";
const hasMetadata = entry => Boolean(entry?.title || entry?.author || entry?.container || entry?.abstract);

const normalizeDoi = doi => String(doi ?? "").trim().toLowerCase();
const toDoiDocId = doi => normalizeDoi(doi).replace(/\//g, '\\');
const isTruthyFlag = value => value === true || value === "true";

const parseDoiInput = (value, splitStrings = false) => {
    if (Array.isArray(value)) {
        return value.flatMap(item => parseDoiInput(item, splitStrings));
    }
    if (typeof value !== "string") {
        return [];
    }

    const doiValues = splitStrings ? value.split(/[\s,;]+/g) : [value];
    return doiValues.map(normalizeDoi).filter(Boolean);
};

const getDoiRequestInput = req => {
    const dois = [];
    let isBulk = false;

    if (Array.isArray(req.query.doi)) {
        isBulk = true;
        dois.push(...parseDoiInput(req.query.doi));
    } else {
        dois.push(...parseDoiInput(req.query.doi));
    }

    if (req.query.dois !== undefined) {
        isBulk = true;
        dois.push(...parseDoiInput(req.query.dois, true));
    }

    if (req.method === "POST") {
        if (Array.isArray(req.body)) {
            isBulk = true;
            dois.push(...parseDoiInput(req.body));
        } else if (req.body && typeof req.body === "object") {
            if (Array.isArray(req.body.doi)) {
                isBulk = true;
            }
            dois.push(...parseDoiInput(req.body.doi));

            if (req.body.dois !== undefined) {
                isBulk = true;
                dois.push(...parseDoiInput(req.body.dois, true));
            }
        }
    }

    return { dois: dois, isBulk: isBulk || dois.length > 1 };
};

const hashPrefetchObservationId = (sourceDoi, targetDoi) =>
    crypto.createHash("sha256").update(`${sourceDoi}\0${targetDoi}`).digest("hex");

const getPublicationRef = doi => firestore.collection('pure-publications').doc(toDoiDocId(doi));
const getPrefetchCandidateRef = doi => firestore.collection('prefetch-candidates').doc(toDoiDocId(doi));

const getPrefetchTargetsByRelation = data => {
    const relationTargets = {
        reference: parseDoiInput(data?.reference, true).slice(0, PREFETCH_MAX_DOIS_PER_RELATION),
        citation: parseDoiInput(data?.citation, true).slice(0, PREFETCH_MAX_DOIS_PER_RELATION)
    };
    const targets = new Map();
    const addTarget = (doi, relationType) => {
        if (!doi) {
            return;
        }
        if (!targets.has(doi)) {
            targets.set(doi, new Set());
        }
        targets.get(doi).add(relationType);
    };
    const maxRelationLength = Math.max(...Object.values(relationTargets).map(dois => dois.length));

    for (let index = 0; index < maxRelationLength; index++) {
        Object.entries(relationTargets).forEach(([relationType, dois]) => addTarget(dois[index], relationType));
    }

    return Array.from(targets.entries()).map(([doi, relationTypes]) => ({
        doi: doi,
        relationTypes: Array.from(relationTypes)
    }));
};

const getLatestDate = (...values) => values
    .map(value => value?.toDate?.() || value)
    .filter(value => value instanceof Date && !Number.isNaN(value.getTime()))
    .sort((a, b) => b.getTime() - a.getTime())[0];

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

const fetchOpenCitationsMeta = async (doi, logEntry) => {
    const timeStartOpenCitationsMeta = new Date().getTime();
    try {
        const responseOpenCitationsMeta = await fetch(`https://api.opencitations.net/meta/v1/metadata/doi:${doi}`, {
            headers: {
                authorization: OPENCITATIONS_ACCESS_TOKEN,
            }
        });
        const dataOpenCitationsMeta = responseOpenCitationsMeta.status === 200 ? (await responseOpenCitationsMeta.json())?.[0] : null;
        logEntry.openCitationsMeta = { status: responseOpenCitationsMeta.status, processingTime: new Date().getTime() - timeStartOpenCitationsMeta };
        return dataOpenCitationsMeta;
    } catch (error) {
        logEntry.openCitationsMeta = {
            status: "error",
            error: String(error).slice(0, 500),
            processingTime: new Date().getTime() - timeStartOpenCitationsMeta
        };
        return null;
    }
};

const fetchOpenAlex = async (doi, logEntry) => {
    if (!OPENALEX_API_KEY) {
        logEntry.openAlex = { status: "skipped", reason: "missing-api-key" };
        return null;
    }

    const timeStartOpenAlex = new Date().getTime();
    try {
        const responseOpenAlex = await fetch(`https://api.openalex.org/works/${encodeURIComponent(`doi:${doi}`)}?api_key=${encodeURIComponent(OPENALEX_API_KEY)}`);
        const dataOpenAlex = responseOpenAlex.status === 200 ? (await responseOpenAlex.json()) : null;
        logEntry.openAlex = { status: responseOpenAlex.status, processingTime: new Date().getTime() - timeStartOpenAlex };
        return dataOpenAlex;
    } catch (error) {
        logEntry.openAlex = {
            status: "error",
            error: String(error).slice(0, 500),
            processingTime: new Date().getTime() - timeStartOpenAlex
        };
        return null;
    }
};

const cleanOpenCitationsAuthor = author => author?.split("; ").map(authorEntry => {
    const orcid = authorEntry.match(/\[.*?orcid:([^\s\]]+)/)?.[1];
    const name = authorEntry.replace(/\s*\[[^\]]*\]/g, "");
    return name + (orcid ? ", " + orcid : "");
}).join("; ");

const cleanOpenCitationsVenue = venue => venue?.replace(/\s*\[[^\]]*\]\s*$/g, "");

const getOpenAlexPage = biblio => {
    if (!biblio?.first_page) {
        return biblio?.last_page;
    }
    return biblio.last_page && biblio.last_page !== biblio.first_page
        ? `${biblio.first_page}-${biblio.last_page}`
        : biblio.first_page;
};

const getOpenAlexAuthors = work => work?.authorships?.map(authorship => {
    const author = authorship.author;
    if (!author?.display_name) {
        return null;
    }
    return author.display_name + (author.orcid ? ", " + author.orcid.replace(/http(s?):\/\/orcid.org\//g, "") : "");
}).filter(Boolean).join("; ");

const getOpenAlexAbstract = work => {
    const invertedIndex = work?.abstract_inverted_index;
    if (!invertedIndex) {
        return undefined;
    }

    return Object.entries(invertedIndex).reduce((words, [word, positions]) => {
        positions.forEach(position => {
            words[position] = word;
        });
        return words;
    }, []).join(" ");
};

const mergeMetadata = (doi, dataCrossref, dataDataCite, dataOpenCitationsMeta, dataOpenAlex) => {
    const data = { doi: doi };
    data.title = dataCrossref?.title?.[0]
        || dataOpenCitationsMeta?.title
        || dataOpenAlex?.title
        || dataOpenAlex?.display_name
        || dataDataCite?.attributes?.titles?.[0]?.title;
    data.subtitle = dataCrossref?.subtitle?.[0];
    data.year = dataCrossref?.published?.['date-parts']?.[0]?.[0]
        || dataOpenCitationsMeta?.pub_date?.match(/^(\d{4})/)?.[1]
        || dataOpenAlex?.publication_year
        || dataDataCite?.attributes?.publicationYear
        || doi.match(/\.((19|20)\d\d)\./)?.[1];
    data.author = dataCrossref?.author?.reduce((acc, author) => acc + author.family
        + (author.given ? ", " + author.given : "")
        + (author.ORCID ? ", " + author.ORCID.replace(/http(s?):\/\/orcid.org\//g, "") : "")
        + "; ", "").slice(0, -2)
        || cleanOpenCitationsAuthor(dataOpenCitationsMeta?.author)
        || getOpenAlexAuthors(dataOpenAlex)
        || dataDataCite?.attributes?.creators?.reduce((acc, author) => acc + author.name + "; ", "").slice(0, -2);
    data.author = data.author?.replace(/(\w)(\w+)(\W?)/g, (match, p1, p2, p3) => p1 + p2.toLowerCase() + p3); // auto-correct ALLCAPS author names
    data.container = dataCrossref?.["container-title"]?.[0]
        || cleanOpenCitationsVenue(dataOpenCitationsMeta?.venue)
        || dataOpenAlex?.primary_location?.source?.display_name
        || dataDataCite?.attributes?.relatedItems?.[0]?.titles?.[0]?.title;
    data.volume = dataCrossref?.volume
        || dataOpenCitationsMeta?.volume
        || dataOpenAlex?.biblio?.volume;
    data.issue = dataCrossref?.issue
        || dataOpenCitationsMeta?.issue
        || dataOpenAlex?.biblio?.issue;
    data.page = dataCrossref?.page
        || dataOpenCitationsMeta?.page
        || getOpenAlexPage(dataOpenAlex?.biblio);
    data.abstract = dataCrossref?.abstract
        || getOpenAlexAbstract(dataOpenAlex)
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
                authorization: OPENCITATIONS_ACCESS_TOKEN,
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
    let dataOpenCitationsMeta = null;
    let dataOpenAlex = null;
    let dataDataCite = null;

    if (!dataCrossref) {
        dataOpenCitationsMeta = await fetchOpenCitationsMeta(doi, logEntry);
    }
    if (!dataCrossref && !hasMetadata(dataOpenCitationsMeta)) {
        dataOpenAlex = await fetchOpenAlex(doi, logEntry);
    }
    if (!dataCrossref && !hasMetadata(dataOpenCitationsMeta) && !hasMetadata(dataOpenAlex)) {
        dataDataCite = await fetchDataCite(doi, logEntry);
    }

    const metadataSource = dataCrossref ? "Crossref"
        : hasMetadata(dataOpenCitationsMeta) ? "OpenCitations Meta"
            : hasMetadata(dataOpenAlex) ? "OpenAlex"
                : dataDataCite ? "DataCite" : "none";

    logEntry.metadataSource = metadataSource;

    let data;
    try {
        data = mergeMetadata(doi, dataCrossref, dataDataCite, dataOpenCitationsMeta, dataOpenAlex);
    } catch (error) {
        console.error("Error processing metadata: " + error + "\n" + JSON.stringify(dataCrossref) + "\n" + JSON.stringify(dataOpenCitationsMeta) + "\n" + JSON.stringify(dataOpenAlex) + "\n" + JSON.stringify(dataDataCite) + "\n" + JSON.stringify(logEntry));
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
        await doiRef.set({ expireAt: getCacheExpireDate(hasMetadata(data) ? 200 : crossref.status), data: data, source: metadataSource });
    }

    return data;
};

const getMetadataAccessToken = async () => {
    const response = await fetch("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", {
        headers: {
            "Metadata-Flavor": "Google",
        }
    });
    if (!response.ok) {
        throw new Error(`Metadata token request failed with ${response.status}`);
    }
    return (await response.json()).access_token;
};

const enqueueHttpTask = async ({ queue, url }) => {
    const accessToken = await getMetadataAccessToken();
    const tasksUrl = `https://cloudtasks.googleapis.com/v2/projects/${PROJECT_ID}/locations/${CLOUD_REGION}/queues/${queue}/tasks`;
    const response = await fetch(tasksUrl, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            task: {
                httpRequest: {
                    httpMethod: "GET",
                    url: url,
                }
            }
        })
    });

    if (!response.ok) {
        throw new Error(`Cloud Tasks enqueue failed with ${response.status}: ${await response.text()}`);
    }

    return response.json();
};

const enqueueRefreshTask = async ({ doi, doiRef, cachedEntry, logEntry }) => {
    const refreshQueuedAt = cachedEntry?.refreshQueuedAt?.toDate?.();
    if (refreshQueuedAt && refreshQueuedAt > new Date(Date.now() - 60 * 1000)) {
        logEntry.refresh = "already-queued";
        logEntry.refreshQueuedAt = refreshQueuedAt.toISOString();
        return;
    }

    const refreshUrl = `${FUNCTION_URL}?doi=${encodeURIComponent(doi)}&refreshCache=true`;
    const task = await enqueueHttpTask({ queue: REFRESH_QUEUE, url: refreshUrl });
    await doiRef.set({ refreshQueuedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    logEntry.refresh = "queued";
    logEntry.refreshTask = task.name;
};

const enqueuePrefetchTask = async ({ doi }) => {
    const prefetchUrl = `${FUNCTION_URL}?doi=${encodeURIComponent(doi)}&refreshCache=true&prefetch=true`;
    return enqueueHttpTask({ queue: PREFETCH_QUEUE, url: prefetchUrl });
};

const markPrefetchEnqueueError = async (doi, error) => {
    await getPrefetchCandidateRef(doi).set({
        status: "enqueue-error",
        failedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastError: String(error).slice(0, 500)
    }, { merge: true });
};

const recordPrefetchCandidate = async ({ sourceDoi, targetDoi, relationTypes, allowQueue }) => {
    const normalizedSourceDoi = normalizeDoi(sourceDoi);
    const normalizedTargetDoi = normalizeDoi(targetDoi);
    const normalizedRelationTypes = relationTypes?.length ? relationTypes : ["related"];

    if (!normalizedTargetDoi || normalizedTargetDoi === normalizedSourceDoi) {
        return { status: "skipped", shouldQueue: false };
    }

    const observationRef = firestore.collection('prefetch-observations')
        .doc(hashPrefetchObservationId(normalizedSourceDoi, normalizedTargetDoi));
    const candidateRef = getPrefetchCandidateRef(normalizedTargetDoi);
    const publicationRef = getPublicationRef(normalizedTargetDoi);

    return firestore.runTransaction(async transaction => {
        const observationDoc = await transaction.get(observationRef);
        if (observationDoc.exists) {
            return { status: "already-observed", shouldQueue: false };
        }

        const candidateDoc = await transaction.get(candidateRef);
        const publicationDoc = await transaction.get(publicationRef);
        const candidate = candidateDoc.exists ? candidateDoc.data() : {};
        const cachedPublication = publicationDoc.exists ? publicationDoc.data() : null;
        const cachedHasMetadata = hasMetadata(cachedPublication?.data);
        const latestHandledAt = getLatestDate(candidate.queuedAt, candidate.fetchedAt, candidate.failedAt);
        const inCooldown = latestHandledAt && latestHandledAt > new Date(Date.now() - PREFETCH_REQUEUE_COOLDOWN_MS);
        const nextUniqueSourceCount = (candidate.uniqueSourceCount || 0) + 1;
        const shouldQueue = Boolean(allowQueue
            && !cachedHasMetadata
            && candidate.status !== "suppressed"
            && nextUniqueSourceCount >= PREFETCH_SIGNAL_THRESHOLD
            && !inCooldown);
        const candidateUpdate = {
            doi: normalizedTargetDoi,
            uniqueSourceCount: admin.firestore.FieldValue.increment(1),
            totalSeenCount: admin.firestore.FieldValue.increment(1),
            lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
            lastSourceDoi: normalizedSourceDoi,
            relationTypes: admin.firestore.FieldValue.arrayUnion(...normalizedRelationTypes)
        };

        if (!candidateDoc.exists || !candidate.firstSeenAt) {
            candidateUpdate.firstSeenAt = admin.firestore.FieldValue.serverTimestamp();
        }
        candidateUpdate.relationCounts = normalizedRelationTypes.reduce((relationCounts, relationType) => {
            relationCounts[relationType] = admin.firestore.FieldValue.increment(1);
            return relationCounts;
        }, {});
        if (cachedHasMetadata) {
            candidateUpdate.status = "cached";
        } else if (shouldQueue) {
            candidateUpdate.status = "queued";
            candidateUpdate.queuedAt = admin.firestore.FieldValue.serverTimestamp();
        } else if (!candidate.status || candidate.status === "cached" || candidate.status === "enqueue-error") {
            candidateUpdate.status = "candidate";
        }

        transaction.set(candidateRef, candidateUpdate, { merge: true });
        transaction.set(observationRef, {
            sourceDoi: normalizedSourceDoi,
            targetDoi: normalizedTargetDoi,
            relationTypes: normalizedRelationTypes,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        return {
            status: shouldQueue ? "queued" : (cachedHasMetadata ? "cached" : "candidate"),
            shouldQueue: shouldQueue,
            uniqueSourceCount: nextUniqueSourceCount,
            targetDoi: normalizedTargetDoi
        };
    });
};

const recordPrefetchSignals = async ({ sourceDoi, data, context, logEntry }) => {
    const normalizedSourceDoi = normalizeDoi(sourceDoi);
    const remainingSignals = Math.max(0, PREFETCH_MAX_SIGNALS_PER_REQUEST - context.signalCount);
    const allTargets = getPrefetchTargetsByRelation(data)
        .filter(target => target.doi !== normalizedSourceDoi);
    const targets = allTargets.slice(0, remainingSignals);
    const summary = {
        signalCount: targets.length,
        queuedCount: 0,
        candidateCount: 0,
        cachedCount: 0,
        duplicateCount: 0,
        skippedCount: 0,
        errorCount: 0,
        enqueueErrorCount: 0
    };

    if (targets.length === 0) {
        if (remainingSignals === 0) {
            logEntry.prefetch = { signalCount: 0, budgetExhausted: true };
        }
        return;
    }

    context.signalCount += targets.length;
    if (allTargets.length > targets.length) {
        summary.budgetExhausted = true;
    }

    for (const target of targets) {
        try {
            const allowQueue = context.enqueueAttemptCount < PREFETCH_MAX_ENQUEUES_PER_REQUEST;
            const result = await recordPrefetchCandidate({
                sourceDoi: normalizedSourceDoi,
                targetDoi: target.doi,
                relationTypes: target.relationTypes,
                allowQueue: allowQueue
            });

            if (result.status === "queued") {
                context.enqueueAttemptCount++;
                try {
                    const task = await enqueuePrefetchTask({ doi: target.doi });
                    await getPrefetchCandidateRef(target.doi).set({
                        prefetchTask: task.name,
                        lastEnqueuedAt: admin.firestore.FieldValue.serverTimestamp()
                    }, { merge: true });
                    summary.queuedCount++;
                } catch (error) {
                    summary.enqueueErrorCount++;
                    summary.lastError = String(error).slice(0, 500);
                    await markPrefetchEnqueueError(target.doi, error);
                }
            } else if (result.status === "cached") {
                summary.cachedCount++;
            } else if (result.status === "already-observed") {
                summary.duplicateCount++;
            } else if (result.status === "skipped") {
                summary.skippedCount++;
            } else {
                summary.candidateCount++;
            }
        } catch (error) {
            summary.errorCount++;
            summary.lastError = String(error).slice(0, 500);
        }
    }

    logEntry.prefetch = summary;
};

const markPrefetchTaskComplete = async ({ doi, data, logEntry }) => {
    const fetched = hasMetadata(data);
    const update = {
        status: fetched ? "fetched" : "failed",
        lastTaskFinishedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastTaskHadMetadata: fetched
    };

    if (fetched) {
        update.fetchedAt = admin.firestore.FieldValue.serverTimestamp();
    } else {
        update.failedAt = admin.firestore.FieldValue.serverTimestamp();
    }

    await getPrefetchCandidateRef(doi).set(update, { merge: true });
    logEntry.prefetchTask = fetched ? "fetched" : "failed";
};

const loadPublicationData = async ({ doi, refreshCache, noCache, collectPrefetchSignals, prefetchContext }) => {
    const timeStart = new Date().getTime();
    const logEntry = { severity: "INFO", doi: doi };
    const bypassCache = noCache || refreshCache;
    const activePrefetchContext = prefetchContext || { signalCount: 0, enqueueAttemptCount: 0 };
    const maybeRecordPrefetchSignals = async data => {
        if (!collectPrefetchSignals || !data) {
            return;
        }
        await recordPrefetchSignals({
            sourceDoi: doi,
            data: data,
            context: activePrefetchContext,
            logEntry: logEntry
        });
    };

    const doiRef = getPublicationRef(doi);
    const doiDoc = await doiRef.get();
    const cachedEntry = doiDoc.exists ? doiDoc.data() : null;
    const cachedData = cachedEntry?.data;
    const cachedExpireAt = cachedEntry?.expireAt?.toDate?.();

    if (!bypassCache && cachedExpireAt > new Date()) {
        logEntry.tag = "cache-hit";
        await maybeRecordPrefetchSignals(cachedData);
        return { data: cachedData, logEntry: logEntry, timeStart: timeStart };
    }

    if (!bypassCache && hasMetadata(cachedData)) {
        logEntry.tag = "cache-stale";
        if (cachedExpireAt) {
            logEntry.cacheExpireAt = cachedExpireAt.toISOString();
        }
        try {
            await enqueueRefreshTask({ doi, doiRef, cachedEntry, logEntry });
        } catch (error) {
            logEntry.refresh = "enqueue-error";
            logEntry.refreshError = String(error).slice(0, 500);
        }
        await maybeRecordPrefetchSignals(cachedData);
        return { data: cachedData, logEntry: logEntry, timeStart: timeStart };
    }

    logEntry.tag = refreshCache ? "cache-refresh" : (noCache ? "cache-disabled" : (cachedEntry ? "cache-expired" : "cache-miss"));
    if (cachedExpireAt) {
        logEntry.cacheExpireAt = cachedExpireAt.toISOString();
    }

    const data = await refreshPublicationData({ doi, doiRef, cachedEntry, noCache, logEntry });
    await maybeRecordPrefetchSignals(data);
    return { data: data, logEntry: logEntry, timeStart: timeStart };
};

const logRequest = (logEntry, data, timeStart) => {
    logEntry.title = data?.title;
    logEntry.processingTime = new Date().getTime() - timeStart;
    console.log(JSON.stringify(logEntry));
};

// Retrieve aggregated data from Crossref (or DataCite) and OpenCitations
// deploy with: "gcloud functions deploy pure-publications --gen2 --runtime=nodejs24 --region=europe-west1 --source=. --entry-point=pure-publications --trigger-http --allow-unauthenticated"
functions.http('pure-publications', async (req, res) => {
    const timeStart = new Date().getTime();

    res.set('Content-Type', 'application/json');
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Methods', 'GET, POST');
    res.set('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === "OPTIONS") {
        // stop preflight requests here
        res.status(204).send('');
        return;
    }

    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).send('Method not allowed');
        return;
    }

    const { dois, isBulk } = getDoiRequestInput(req);
    if (dois.length === 0) {
        res.status(400).send('Missing DOI parameter');
        return;
    }

    if (dois.length > MAX_BULK_DOIS) {
        res.status(400).send(`Too many DOI parameters; maximum is ${MAX_BULK_DOIS}`);
        return;
    }

    const refreshCache = isTruthyFlag(req.query.refreshCache) || isTruthyFlag(req.body?.refreshCache);
    const noCache = isTruthyFlag(req.query.noCache) || isTruthyFlag(req.body?.noCache);
    const prefetch = isTruthyFlag(req.query.prefetch) || isTruthyFlag(req.body?.prefetch);
    const collectPrefetchSignals = !refreshCache && !prefetch;
    const prefetchContext = { signalCount: 0, enqueueAttemptCount: 0 };

    if (isBulk) {
        const results = [];
        for (const doi of dois) {
            const result = await loadPublicationData({
                doi,
                refreshCache,
                noCache,
                collectPrefetchSignals,
                prefetchContext
            });
            if (prefetch) {
                try {
                    await markPrefetchTaskComplete({ doi, data: result.data, logEntry: result.logEntry });
                } catch (error) {
                    result.logEntry.prefetchTask = "mark-complete-error";
                    result.logEntry.prefetchTaskError = String(error).slice(0, 500);
                }
            }
            logRequest(result.logEntry, result.data, result.timeStart);
            results.push(result.data);
        }

        console.log(JSON.stringify({
            severity: "INFO",
            tag: "bulk",
            doiCount: dois.length,
            prefetch: prefetch || undefined,
            prefetchSignals: collectPrefetchSignals ? prefetchContext.signalCount : undefined,
            prefetchEnqueueAttempts: collectPrefetchSignals ? prefetchContext.enqueueAttemptCount : undefined,
            processingTime: new Date().getTime() - timeStart
        }));
        res.send(results);
        return;
    }

    const result = await loadPublicationData({
        doi: dois[0],
        refreshCache,
        noCache,
        collectPrefetchSignals,
        prefetchContext
    });
    if (prefetch) {
        try {
            await markPrefetchTaskComplete({ doi: dois[0], data: result.data, logEntry: result.logEntry });
        } catch (error) {
            result.logEntry.prefetchTask = "mark-complete-error";
            result.logEntry.prefetchTaskError = String(error).slice(0, 500);
        }
    }
    logRequest(result.logEntry, result.data, result.timeStart);
    res.send(result.data);
});
